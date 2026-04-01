#!/usr/bin/env python3
"""GA Cluster Resolution — Priority email extraction engine.

Processes 236K leads from leads.db by streaming 20KB HTML fingerprints,
building tracking-ID clusters, propagating discovered emails across
cluster members, and upgrading lead tiers.

Usage:
    python scripts/ga_cluster_resolve.py --db data/leads.db --tier 2 --limit 500
    python scripts/ga_cluster_resolve.py --resume --workers 15
    python scripts/ga_cluster_resolve.py --tier 3 --limit 5000 --offset 2554
"""
import argparse
import json
import os
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CHECKPOINT_PATH = DATA_DIR / ".cluster_checkpoint.json"
REPORT_PATH = DATA_DIR / "cluster-resolution-report.json"

MAX_BYTES = 20480
CONNECT_TIMEOUT, READ_TIMEOUT = 5, 10
BATCH_COMMIT = 100
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}

# ── Regex patterns ────────────────────────────────────────────────────────
RE_GA_UA = re.compile(r"UA-\d{4,10}-\d{1,4}")
RE_GA4 = re.compile(r"G-[A-Z0-9]{10,12}")
RE_GTM = re.compile(r"GTM-[A-Z0-9]{6,8}")
RE_FB_PIXEL = re.compile(r"fbq\(['\"]init['\"],\s*['\"](\d+)['\"]")
RE_GADS = re.compile(r"AW-\d{9,12}")
RE_ADSENSE = re.compile(r"ca-pub-\d{10,16}")
RE_EMAIL = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}")
RE_PHONE = re.compile(r"(\+?1[-.]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
RE_JSONLD = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)
RE_MAILTO = re.compile(r'mailto:([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,})')

TRADE_KEYWORDS = {
    "plumb", "electric", "hvac", "roofing", "landscap", "contracting",
    "construction", "renovati", "painting", "flooring", "fencing",
    "paving", "concrete", "demoliti", "excavat", "weld", "carpent",
    "mechanic", "locksmith", "pest control", "clean", "towing",
    "moving", "tree", "solar", "insulation", "drywall", "masonry",
    "siding", "gutter", "window", "garage door", "appliance repair",
}
LOCAL_BIZ_TYPES = {
    "localbusiness", "store", "restaurant", "plumber", "electrician",
    "hvacbusiness", "roofingcontractor", "generalcontractor",
    "homeandconstructionbusiness", "professionalservice",
    "autorepair", "beautysalon", "dentist", "medicalclinic",
}
EMAIL_BLACKLIST_SUFFIXES = (".png", ".jpg", ".gif", ".svg", ".webp", ".css", ".js")
EMAIL_BLACKLIST_PARTS = ("example.com", "wixpress", "sentry", "schema.org", "w3.org")


# ── Database ──────────────────────────────────────────────────────────────

def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def fetch_priority_queue(conn: sqlite3.Connection, tier: Optional[int],
                         limit: int, offset: int) -> list[dict]:
    """Build prioritized lead queue for processing."""
    leads = []
    if tier:
        rows = conn.execute(
            "SELECT * FROM leads WHERE tier = ? AND suppress = 0 "
            "ORDER BY confidence_score DESC LIMIT ? OFFSET ?",
            (tier, limit, offset),
        ).fetchall()
        return [dict(r) for r in rows]

    # Priority 1: tier 2 (have some data, need email)
    remaining = limit
    rows = conn.execute(
        "SELECT * FROM leads WHERE tier = 2 AND suppress = 0 "
        "AND (email IS NULL OR email = '') "
        "ORDER BY confidence_score DESC LIMIT ? OFFSET ?",
        (remaining, offset),
    ).fetchall()
    leads.extend(dict(r) for r in rows)
    remaining -= len(rows)
    if remaining <= 0:
        return leads[:limit]

    # Priority 2: GMB high volume with domains
    rows = conn.execute(
        "SELECT * FROM leads WHERE review_count > 20 AND suppress = 0 "
        "AND (domain IS NOT NULL AND domain != '') "
        "AND (email IS NULL OR email = '') "
        "ORDER BY review_count DESC LIMIT ?",
        (remaining,),
    ).fetchall()
    seen_ids = {l["id"] for l in leads}
    for r in rows:
        if dict(r)["id"] not in seen_ids:
            leads.append(dict(r))
            remaining -= 1
            if remaining <= 0:
                break

    if remaining <= 0:
        return leads[:limit]

    # Priority 3: permit-verified with company names
    rows = conn.execute(
        "SELECT * FROM leads WHERE permit_count > 0 AND suppress = 0 "
        "AND business_name IS NOT NULL AND business_name != '' "
        "AND (email IS NULL OR email = '') "
        "ORDER BY permit_count DESC LIMIT ?",
        (remaining,),
    ).fetchall()
    seen_ids = {l["id"] for l in leads}
    for r in rows:
        if dict(r)["id"] not in seen_ids:
            leads.append(dict(r))
            remaining -= 1
            if remaining <= 0:
                break

    if remaining <= 0:
        return leads[:limit]

    # Priority 4: remaining tier 3
    rows = conn.execute(
        "SELECT * FROM leads WHERE tier = 3 AND suppress = 0 "
        "AND (domain IS NOT NULL AND domain != '') "
        "AND (email IS NULL OR email = '') "
        "ORDER BY confidence_score DESC LIMIT ?",
        (remaining,),
    ).fetchall()
    seen_ids = {l["id"] for l in leads}
    for r in rows:
        if dict(r)["id"] not in seen_ids:
            leads.append(dict(r))
            remaining -= 1
            if remaining <= 0:
                break

    return leads[:limit]


# ── HTML Extraction ───────────────────────────────────────────────────────

def _is_valid_email(email: str) -> bool:
    if email.endswith(EMAIL_BLACKLIST_SUFFIXES):
        return False
    return not any(part in email for part in EMAIL_BLACKLIST_PARTS)


def _extract_schema(html: str) -> dict:
    info = {"emails": [], "phones": [], "is_local": False}
    for m in RE_JSONLD.finditer(html):
        try:
            data = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            types = item.get("@type", "")
            types = types if isinstance(types, list) else [types]
            if any(t.lower().replace(" ", "") in LOCAL_BIZ_TYPES for t in types):
                info["is_local"] = True
            email = item.get("email", "")
            if email and _is_valid_email(email):
                info["emails"].append(email.strip().lower())
            phone = item.get("telephone", "")
            if phone:
                info["phones"].append(phone.strip())
    return info


def _has_trade_signal(html: str) -> bool:
    lower = html[:8000].lower()
    title_m = re.search(r"<title[^>]*>(.*?)</title>", lower, re.DOTALL)
    desc_m = re.search(r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']*)', lower)
    h1_m = re.search(r"<h1[^>]*>(.*?)</h1>", lower, re.DOTALL)
    combined = " ".join(filter(None, [
        title_m.group(1) if title_m else "",
        desc_m.group(1) if desc_m else "",
        h1_m.group(1) if h1_m else "",
    ]))
    return any(kw in combined for kw in TRADE_KEYWORDS)


def fingerprint_domain(domain: str) -> dict:
    """Stream first 20KB from domain, extract all tracking IDs and signals."""
    result = {
        "domain": domain, "ga_ids": [], "gtm_ids": [], "pixel_ids": [],
        "gads_ids": [], "adsense_ids": [], "emails": [], "phones": [],
        "has_schema_local": False, "has_trade_keywords": False,
        "response_ms": 0, "server": "", "error": None,
    }
    start = time.time()

    html = ""
    for url, verify in [(f"https://{domain}", False), (f"http://{domain}", True)]:
        try:
            resp = requests.get(
                url, headers=HEADERS, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                stream=True, allow_redirects=True, verify=verify,
            )
            result["server"] = resp.headers.get("Server", "")
            chunks, total = [], 0
            for chunk in resp.iter_content(chunk_size=4096):
                chunks.append(chunk)
                total += len(chunk)
                if total >= MAX_BYTES:
                    break
            resp.close()
            html = b"".join(chunks)[:MAX_BYTES].decode("utf-8", errors="replace")
            break
        except requests.exceptions.SSLError:
            continue
        except Exception as exc:
            result["error"] = str(exc)[:120]
            break

    result["response_ms"] = int((time.time() - start) * 1000)
    if not html:
        return result

    # Tracking IDs
    result["ga_ids"] = list(set(RE_GA_UA.findall(html) + RE_GA4.findall(html)))
    result["gtm_ids"] = list(set(RE_GTM.findall(html)))
    result["pixel_ids"] = list(set(RE_FB_PIXEL.findall(html)))
    result["gads_ids"] = list(set(RE_GADS.findall(html)))
    result["adsense_ids"] = list(set(RE_ADSENSE.findall(html)))

    # Schema.org
    schema = _extract_schema(html)
    result["has_schema_local"] = schema["is_local"]
    result["emails"].extend(schema["emails"])
    result["phones"].extend(schema["phones"])

    # Mailto links
    for email in RE_MAILTO.findall(html):
        if _is_valid_email(email):
            result["emails"].append(email.strip().lower())

    # Text emails
    for email in RE_EMAIL.findall(html):
        if _is_valid_email(email):
            result["emails"].append(email.strip().lower())

    # Phones from HTML
    for phone in RE_PHONE.findall(html):
        if phone.strip():
            result["phones"].append(phone.strip())

    # Deduplicate
    result["emails"] = list(dict.fromkeys(result["emails"]))
    result["phones"] = list(dict.fromkeys(result["phones"]))

    # Trade keywords
    result["has_trade_keywords"] = _has_trade_signal(html)
    return result


# ── Cluster Graph ─────────────────────────────────────────────────────────

class ClusterGraph:
    """In-memory tracking-ID cluster graph for email propagation."""

    def __init__(self):
        self.clusters: dict[str, dict] = {}  # id -> {domains, emails, phones}

    def add(self, tracking_id: str, domain: str, emails: list, phones: list):
        if tracking_id not in self.clusters:
            self.clusters[tracking_id] = {
                "domains": set(), "emails": set(), "phones": set(),
            }
        c = self.clusters[tracking_id]
        c["domains"].add(domain)
        c["emails"].update(e for e in emails if e)
        c["phones"].update(p for p in phones if p)

    def propagate(self) -> dict[str, str]:
        """Propagate emails across cluster members. Returns {domain: email}."""
        domain_emails: dict[str, str] = {}
        for cluster in self.clusters.values():
            if not cluster["emails"]:
                continue
            best_email = next(iter(cluster["emails"]))
            for domain in cluster["domains"]:
                if domain not in domain_emails:
                    domain_emails[domain] = best_email
        return domain_emails

    def stats(self) -> dict:
        multi = sum(1 for c in self.clusters.values() if len(c["domains"]) >= 2)
        return {
            "total_clusters": len(self.clusters),
            "multi_domain_clusters": multi,
            "domains_in_clusters": sum(len(c["domains"]) for c in self.clusters.values()),
        }


# ── Checkpoint ────────────────────────────────────────────────────────────

def load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        with open(CHECKPOINT_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"processed_ids": [], "stats": {}}


def save_checkpoint(processed_ids: list[int], stats: dict):
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump({"processed_ids": processed_ids, "stats": stats}, f)


# ── Main Processing ──────────────────────────────────────────────────────

def process_batch(conn: sqlite3.Connection, leads: list[dict],
                  workers: int, delay: float) -> dict:
    """Process a batch of leads: fingerprint, cluster, classify, write."""
    graph = ClusterGraph()
    stats = {
        "domains_scanned": 0, "emails_direct": 0, "emails_propagated": 0,
        "tier_upgrades_3to2": 0, "tier_upgrades_2to1": 0,
        "ga_clusters": 0, "schema_hits": 0, "errors": 0,
    }

    # Map domain -> list of lead IDs for this batch
    domain_leads: dict[str, list[int]] = {}
    lead_map: dict[int, dict] = {}
    domains_to_scan: list[str] = []

    for lead in leads:
        lead_map[lead["id"]] = lead
        domain = (lead.get("domain") or lead.get("website") or "").strip()
        if domain:
            domain = domain.replace("https://", "").replace("http://", "").split("/")[0]
            domain_leads.setdefault(domain, []).append(lead["id"])
            if domain not in {d for d in domains_to_scan}:
                domains_to_scan.append(domain)

    print(f"  Scanning {len(domains_to_scan)} unique domains for {len(leads)} leads...")

    # STEP 1: Parallel HTTP fingerprinting
    fingerprints: dict[str, dict] = {}
    completed = 0
    host_last_request: dict[str, float] = {}

    def throttled_fingerprint(domain: str) -> dict:
        now = time.time()
        last = host_last_request.get(domain, 0)
        if now - last < delay:
            time.sleep(delay - (now - last))
        host_last_request[domain] = time.time()
        return fingerprint_domain(domain)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(throttled_fingerprint, d): d for d in domains_to_scan}
        for future in as_completed(futures):
            domain = futures[future]
            completed += 1
            try:
                fp = future.result()
                fingerprints[domain] = fp
                stats["domains_scanned"] += 1
                if fp["has_schema_local"]:
                    stats["schema_hits"] += 1
                tag = "OK" if not fp["error"] else "ERR"
                ids_str = ""
                if fp["ga_ids"]:
                    ids_str = f" GA:{fp['ga_ids'][0]}"
                if fp["emails"]:
                    ids_str += f" email:{fp['emails'][0]}"
                print(f"    [{completed}/{len(domains_to_scan)}] {tag} {domain} "
                      f"{fp['response_ms']}ms{ids_str}")
            except Exception as exc:
                stats["errors"] += 1
                print(f"    [{completed}/{len(domains_to_scan)}] FAIL {domain}: {exc}")

    # STEP 2: Build cluster graph
    for domain, fp in fingerprints.items():
        all_ids = fp["ga_ids"] + fp["gtm_ids"] + fp["pixel_ids"] + fp["gads_ids"] + fp["adsense_ids"]
        for tid in all_ids:
            graph.add(tid, domain, fp["emails"], fp["phones"])

    propagated_emails = graph.propagate()
    cluster_stats = graph.stats()
    stats["ga_clusters"] = cluster_stats["multi_domain_clusters"]

    # STEP 3 + 4: Classify and write results
    cur = conn.cursor()
    batch_count = 0
    processed_ids = []

    for lead in leads:
        lead_id = lead["id"]
        processed_ids.append(lead_id)
        domain = (lead.get("domain") or lead.get("website") or "").strip()
        if domain:
            domain = domain.replace("https://", "").replace("http://", "").split("/")[0]

        fp = fingerprints.get(domain, {})
        old_tier = lead.get("tier", 3)
        new_email = None
        new_phone = None

        # Direct email from fingerprint
        if fp.get("emails"):
            new_email = fp["emails"][0]
            stats["emails_direct"] += 1

        # Propagated email from cluster
        if not new_email and domain in propagated_emails:
            new_email = propagated_emails[domain]
            stats["emails_propagated"] += 1

        # Phone from fingerprint
        if fp.get("phones"):
            new_phone = fp["phones"][0]

        # Binary classify
        has_schema = fp.get("has_schema_local", False)
        has_trade = fp.get("has_trade_keywords", False)
        has_phone = bool(new_phone or lead.get("phone"))
        has_email = bool(new_email or lead.get("email"))

        if has_schema and has_trade and has_phone:
            new_tier = 1
        elif has_email:
            new_tier = max(min(old_tier, 2), 1) if has_schema else 2
        else:
            new_tier = old_tier

        # Confidence score
        score = lead.get("confidence_score", 0.0) or 0.0
        if has_email:
            score = max(score, 0.25)
        if has_phone:
            score = max(score, score + 0.10)
        if has_schema:
            score = max(score, score + 0.15)
        if has_trade:
            score = max(score, score + 0.10)
        score = min(round(score, 3), 1.0)

        # Track tier upgrades
        if old_tier == 3 and new_tier == 2:
            stats["tier_upgrades_3to2"] += 1
        elif old_tier == 3 and new_tier == 1:
            stats["tier_upgrades_3to2"] += 1
            stats["tier_upgrades_2to1"] += 1
        elif old_tier == 2 and new_tier == 1:
            stats["tier_upgrades_2to1"] += 1

        # Update leads.db
        cur.execute(
            """UPDATE leads SET
                email = COALESCE(NULLIF(?, ''), email),
                phone = COALESCE(NULLIF(?, ''), phone),
                tier = ?, confidence_score = ?,
                domain = COALESCE(NULLIF(?, ''), domain)
            WHERE id = ?""",
            (new_email or "", new_phone or "", new_tier, score, domain, lead_id),
        )

        # Update owner_fingerprints for each tracking ID
        all_ids = fp.get("ga_ids", []) + fp.get("gtm_ids", [])
        for tid in all_ids[:1]:  # Primary ID only
            cur.execute(
                """INSERT OR REPLACE INTO owner_fingerprints
                (ga_id, gtm_id, pixel_id, adsense_id, domains,
                 confirmed_email, confirmed_phone, cluster_confidence_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    fp.get("ga_ids", [""])[0] if fp.get("ga_ids") else tid,
                    fp.get("gtm_ids", [""])[0] if fp.get("gtm_ids") else "",
                    fp.get("pixel_ids", [""])[0] if fp.get("pixel_ids") else "",
                    fp.get("adsense_ids", [""])[0] if fp.get("adsense_ids") else "",
                    domain,
                    new_email or lead.get("email", ""),
                    new_phone or lead.get("phone", ""),
                    score,
                ),
            )

        batch_count += 1
        if batch_count % BATCH_COMMIT == 0:
            conn.commit()
            save_checkpoint(processed_ids, stats)
            print(f"    -- committed {batch_count} records, checkpoint saved")

    conn.commit()
    return stats


def main():
    ap = argparse.ArgumentParser(description="GA Cluster Resolution — Priority email extraction")
    ap.add_argument("--db", default=str(DATA_DIR / "leads.db"), help="SQLite database path")
    ap.add_argument("--tier", type=int, choices=[1, 2, 3], help="Process specific tier only")
    ap.add_argument("--limit", type=int, default=500, help="Max leads to process (default 500)")
    ap.add_argument("--offset", type=int, default=0, help="Offset into result set")
    ap.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    ap.add_argument("--workers", type=int, default=15, help="HTTP fetch workers (default 15)")
    ap.add_argument("--delay", type=float, default=0.3, help="Per-host delay in seconds")
    args = ap.parse_args()

    db_path = args.db
    if not Path(db_path).exists():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = get_conn(db_path)

    # Load checkpoint for resume
    skip_ids: set[int] = set()
    if args.resume:
        ckpt = load_checkpoint()
        skip_ids = set(ckpt.get("processed_ids", []))
        print(f"Resuming: {len(skip_ids)} leads already processed")

    # Fetch priority queue
    leads = fetch_priority_queue(conn, args.tier, args.limit + len(skip_ids), args.offset)
    if skip_ids:
        leads = [l for l in leads if l["id"] not in skip_ids]
    leads = leads[:args.limit]

    if not leads:
        print("No leads to process.")
        conn.close()
        return

    tier_dist = {}
    for l in leads:
        tier_dist[l.get("tier", 3)] = tier_dist.get(l.get("tier", 3), 0) + 1
    print(f"Processing {len(leads)} leads  (tier distribution: {tier_dist})")
    print(f"Workers: {args.workers}  |  Delay: {args.delay}s  |  Batch commit: {BATCH_COMMIT}")
    print()

    start = time.time()
    stats = process_batch(conn, leads, args.workers, args.delay)
    elapsed = time.time() - start

    # Write report
    report = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "elapsed_seconds": round(elapsed, 1),
        "leads_processed": len(leads),
        **stats,
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # Clean checkpoint on success
    if CHECKPOINT_PATH.exists():
        os.remove(CHECKPOINT_PATH)

    conn.close()

    # Print summary
    print(f"\n{'='*60}")
    print(f"  GA Cluster Resolution Complete")
    print(f"{'='*60}")
    print(f"  Leads processed:      {len(leads)}")
    print(f"  Domains scanned:      {stats['domains_scanned']}")
    print(f"  Emails (direct):      {stats['emails_direct']}")
    print(f"  Emails (propagated):  {stats['emails_propagated']}")
    print(f"  Tier upgrades 3->2:   {stats['tier_upgrades_3to2']}")
    print(f"  Tier upgrades 2->1:   {stats['tier_upgrades_2to1']}")
    print(f"  GA clusters (2+ dom): {stats['ga_clusters']}")
    print(f"  Schema.org hits:      {stats['schema_hits']}")
    print(f"  Errors:               {stats['errors']}")
    print(f"  Elapsed:              {elapsed:.1f}s")
    print(f"  Report:               {REPORT_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
