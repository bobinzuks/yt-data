#!/usr/bin/env python3
"""Agent 9 — Permit Fingerprint MinCut Cross-Reference Engine.

Takes contractor records from permits, cross-references multiple sources
(DNS, website scraping, SSL certs, Google dorks) to find verified emails
with highest confidence via graph-based scoring.
"""

import argparse
import csv
import json
import os
import re
import socket
import sys
import time
from pathlib import Path
from typing import Optional

import dns.resolver
import requests

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CHECKPOINT_PATH = DATA_DIR / "fingerprint-checkpoint.json"
OUTPUT_CSV = DATA_DIR / "permit-fingerprints.csv"
OUTPUT_JSON = DATA_DIR / "fingerprint-summary.json"
WEIGHTS_PATH = DATA_DIR / "fingerprint_weights.json"

EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
}
REQUEST_DELAY = 2.0


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_ontario(path: str) -> list[dict]:
    """Load ontario-registries.csv into unified records."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append({
                "company_name": r.get("company_name", "").strip(),
                "license_number": r.get("license_number", "").strip(),
                "trade_type": r.get("trade_type", "").strip(),
                "city": r.get("city", "").strip(),
                "email": r.get("email", "").strip(),
                "phone": r.get("phone", "").strip(),
                "source_file": "ontario-registries.csv",
            })
    return rows


def load_victoria(path: str) -> list[dict]:
    """Load victoria-permit-emails.csv into unified records."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append({
                "company_name": r.get("company", "").strip(),
                "license_number": "",
                "trade_type": r.get("permit_type", "").strip(),
                "city": "Victoria",
                "email": r.get("email", "").strip(),
                "phone": r.get("phone", "").strip(),
                "source_file": "victoria-permit-emails.csv",
            })
    return rows


def load_records(input_path: str) -> list[dict]:
    """Load records from the given CSV, auto-detecting format."""
    p = Path(input_path)
    if "victoria" in p.name.lower():
        return load_victoria(input_path)
    return load_ontario(input_path)


# ---------------------------------------------------------------------------
# Cross-reference helpers
# ---------------------------------------------------------------------------

def clean_company(name: str) -> str:
    """Strip suffixes and non-alpha chars for domain guessing."""
    n = name.lower()
    for w in ("ltd", "inc", "corp", "llc", "co", "limited", ".", ","):
        n = n.replace(w, "")
    return re.sub(r"[^a-z0-9]", "", n)


def dns_resolves(domain: str) -> bool:
    """Check if a domain has A or MX records."""
    for qtype in ("A", "MX"):
        try:
            dns.resolver.resolve(domain, qtype, lifetime=3)
            return True
        except Exception:
            pass
    return False


def discover_domain(company_name: str) -> Optional[str]:
    """Try candidate domains and return the first that resolves."""
    slug = clean_company(company_name)
    if not slug:
        return None
    candidates = [
        f"{slug}.ca", f"{slug}.com",
        f"{slug}ltd.ca", f"{slug}inc.ca",
    ]
    for d in candidates:
        if dns_resolves(d):
            return d
    return None


def scrape_emails_from_url(url: str) -> list[str]:
    """GET a URL, extract email addresses from body text."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=8, allow_redirects=True)
        if resp.status_code == 200:
            return list(set(EMAIL_RE.findall(resp.text)))
    except Exception:
        pass
    return []


def scrape_domain_emails(domain: str) -> list[str]:
    """Scrape homepage and /contact for emails."""
    found = []
    for path in ("", "/contact", "/contact-us", "/about"):
        url = f"https://{domain}{path}"
        found.extend(scrape_emails_from_url(url))
        time.sleep(0.5)
    return list(set(found))


def google_dork_emails(company_name: str, city: str) -> list[str]:
    """Search via Google custom-search-like fallback for emails."""
    query = f'"{company_name}" "{city}" email'
    url = "https://html.duckduckgo.com/html/"
    try:
        resp = requests.post(
            url, data={"q": query}, headers=HEADERS, timeout=10
        )
        if resp.status_code == 200:
            return list(set(EMAIL_RE.findall(resp.text)))
    except Exception:
        pass
    return []


def check_ssl_cert(domain: str, company_name: str) -> bool:
    """Query crt.sh for SSL certificates matching the domain."""
    try:
        resp = requests.get(
            f"https://crt.sh/?q={domain}&output=json",
            headers=HEADERS, timeout=10,
        )
        if resp.status_code == 200:
            certs = resp.json()
            slug = clean_company(company_name)
            for cert in certs[:20]:
                cn = cert.get("common_name", "").lower()
                if slug and slug in cn.replace(".", "").replace("-", ""):
                    return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Graph scoring
# ---------------------------------------------------------------------------

EDGE_WEIGHTS = {
    "domain_resolves": 0.3,
    "email_on_website": 0.8,
    "ssl_cert_match": 0.5,
    "google_dork_match": 0.4,
}


def compute_confidence(edges: dict[str, bool]) -> float:
    """Sum edge weights for matched sources, cap at 1.0."""
    score = sum(
        EDGE_WEIGHTS[k] for k, v in edges.items() if v and k in EDGE_WEIGHTS
    )
    return min(score, 1.0)


def classify_tier(confidence: float) -> str:
    if confidence >= 0.7:
        return "tier1_verified"
    elif confidence >= 0.4:
        return "tier2_likely"
    return "tier3_unverified"


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_contractor(rec: dict) -> dict:
    """Run full cross-reference pipeline for one contractor record."""
    edges: dict[str, bool] = {
        "domain_resolves": False,
        "email_on_website": False,
        "ssl_cert_match": False,
        "google_dork_match": False,
    }
    domain = None
    best_email = rec.get("email", "") or ""
    sources = []

    # 1. Domain discovery
    domain = discover_domain(rec["company_name"])
    if domain:
        edges["domain_resolves"] = True
        sources.append("dns")
        time.sleep(REQUEST_DELAY)

        # Scrape website for emails
        site_emails = scrape_domain_emails(domain)
        if site_emails:
            edges["email_on_website"] = True
            sources.append("website")
            if not best_email:
                best_email = site_emails[0]
        time.sleep(REQUEST_DELAY)

        # SSL cert check
        if check_ssl_cert(domain, rec["company_name"]):
            edges["ssl_cert_match"] = True
            sources.append("ssl_cert")
        time.sleep(REQUEST_DELAY)

    # 2. Google dork
    dork_emails = google_dork_emails(rec["company_name"], rec["city"])
    if dork_emails:
        edges["google_dork_match"] = True
        sources.append("google_dork")
        if not best_email:
            best_email = dork_emails[0]
    time.sleep(REQUEST_DELAY)

    confidence = compute_confidence(edges)
    tier = classify_tier(confidence)

    return {
        "company_name": rec["company_name"],
        "email": best_email,
        "phone": rec.get("phone", ""),
        "domain": domain or "",
        "confidence": round(confidence, 2),
        "sources_matched": ",".join(sources),
        "trade": rec.get("trade_type", ""),
        "city": rec.get("city", ""),
        "license_number": rec.get("license_number", ""),
        "tier": tier,
    }


# ---------------------------------------------------------------------------
# Checkpointing
# ---------------------------------------------------------------------------

def load_checkpoint() -> tuple[int, list[dict]]:
    """Return (last_index, results) from checkpoint file."""
    if CHECKPOINT_PATH.exists():
        data = json.loads(CHECKPOINT_PATH.read_text())
        return data.get("last_index", 0), data.get("results", [])
    return 0, []


def save_checkpoint(index: int, results: list[dict]):
    CHECKPOINT_PATH.write_text(json.dumps({
        "last_index": index,
        "results": results,
    }, indent=2))


# ---------------------------------------------------------------------------
# GNN fingerprint model (simple pattern predictor)
# ---------------------------------------------------------------------------

TRADE_WORDS = {"electric", "plumb", "hvac", "mechan", "fire", "roof", "build"}


def extract_features(rec: dict) -> list[float]:
    """Extract numeric features from a contractor record."""
    name = rec.get("company_name", "").lower()
    city = rec.get("city", "").lower()
    trade = rec.get("trade_type", "").lower() if rec.get("trade_type") else ""
    return [
        len(name),
        1.0 if any(w in name for w in TRADE_WORDS) else 0.0,
        1.0 if city and city in name else 0.0,
        hash(trade) % 50 / 50.0,
        hash(city) % 50 / 50.0,
    ]


def email_pattern(email: str) -> str:
    """Classify the local-part pattern of an email."""
    if not email or "@" not in email:
        return "unknown"
    local = email.split("@")[0].lower()
    if local == "info":
        return "info@"
    if local == "admin":
        return "admin@"
    if local in ("contact", "hello", "office"):
        return "generic@"
    if "." in local:
        return "firstname.last@"
    return "firstname@"


def train_fingerprint_model(results: list[dict]):
    """Train a simple weight-per-feature model on confirmed matches."""
    confirmed = [r for r in results if r["confidence"] >= 0.7 and r["email"]]
    if len(confirmed) < 10:
        print(f"  Only {len(confirmed)} confirmed — need 10+ to train. Skipping.")
        return

    patterns: dict[str, list[list[float]]] = {}
    for r in confirmed:
        pat = email_pattern(r["email"])
        feats = extract_features(r)
        patterns.setdefault(pat, []).append(feats)

    # Compute mean feature vector per pattern
    centroids = {}
    for pat, feat_lists in patterns.items():
        n = len(feat_lists)
        dim = len(feat_lists[0])
        centroid = [sum(f[i] for f in feat_lists) / n for i in range(dim)]
        centroids[pat] = {"centroid": centroid, "count": n}

    weights = {
        "centroids": centroids,
        "total_confirmed": len(confirmed),
        "feature_names": [
            "name_length", "has_trade_word", "has_city_word",
            "trade_encoded", "city_encoded",
        ],
    }
    WEIGHTS_PATH.write_text(json.dumps(weights, indent=2))
    print(f"  Model saved: {len(centroids)} patterns from {len(confirmed)} records.")


def predict_pattern(rec: dict) -> tuple[str, float]:
    """Predict email pattern for a record using saved weights."""
    if not WEIGHTS_PATH.exists():
        return "unknown", 0.0
    weights = json.loads(WEIGHTS_PATH.read_text())
    centroids = weights.get("centroids", {})
    if not centroids:
        return "unknown", 0.0

    feats = extract_features(rec)
    best_pat, best_dist = "unknown", float("inf")
    for pat, info in centroids.items():
        c = info["centroid"]
        dist = sum((a - b) ** 2 for a, b in zip(feats, c)) ** 0.5
        if dist < best_dist:
            best_dist = dist
            best_pat = pat

    confidence = max(0.0, 1.0 - best_dist / 10.0)
    return best_pat, round(confidence, 2)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_csv(results: list[dict], min_confidence: float):
    """Write filtered results to CSV."""
    filtered = [r for r in results if r["confidence"] >= min_confidence]
    fields = [
        "company_name", "email", "phone", "domain", "confidence",
        "sources_matched", "trade", "city", "license_number",
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(filtered)
    print(f"Wrote {len(filtered)} rows to {OUTPUT_CSV}")


def write_summary(results: list[dict]):
    """Write summary JSON with tier breakdown."""
    tiers = {"tier1_verified": 0, "tier2_likely": 0, "tier3_unverified": 0}
    emails_found = 0
    domains_found = 0
    for r in results:
        tiers[r.get("tier", "tier3_unverified")] += 1
        if r.get("email"):
            emails_found += 1
        if r.get("domain"):
            domains_found += 1

    summary = {
        "total_processed": len(results),
        "emails_found": emails_found,
        "domains_found": domains_found,
        "tier_breakdown": tiers,
        "avg_confidence": round(
            sum(r["confidence"] for r in results) / max(len(results), 1), 3
        ),
    }
    OUTPUT_JSON.write_text(json.dumps(summary, indent=2))
    print(f"Summary written to {OUTPUT_JSON}")
    for k, v in summary.items():
        print(f"  {k}: {v}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Agent 9 — Permit Fingerprint MinCut Cross-Reference"
    )
    parser.add_argument(
        "--input", default=str(DATA_DIR / "ontario-registries.csv"),
        help="CSV path (default: data/ontario-registries.csv)",
    )
    parser.add_argument("--limit", type=int, default=500,
                        help="Max contractors to process (default: 500)")
    parser.add_argument("--min-confidence", type=float, default=0.5,
                        help="Min confidence to include in output (default: 0.5)")
    parser.add_argument("--train", action="store_true",
                        help="Train GNN fingerprint model after cross-referencing")
    parser.add_argument("--no-resume", action="store_true",
                        help="Ignore existing checkpoint and start fresh")
    args = parser.parse_args()

    # Also load Victoria permits if available and merge
    records = load_records(args.input)
    vic_path = DATA_DIR / "victoria-permit-emails.csv"
    if vic_path.exists() and "victoria" not in Path(args.input).name.lower():
        records.extend(load_victoria(str(vic_path)))

    # Deduplicate by company name
    seen = set()
    unique = []
    for r in records:
        key = r["company_name"].lower().strip()
        if key and key not in seen:
            seen.add(key)
            unique.append(r)
    records = unique[: args.limit]
    print(f"Loaded {len(records)} unique contractors (limit={args.limit})")

    # Resume from checkpoint
    start_idx, results = (0, []) if args.no_resume else load_checkpoint()
    if start_idx > 0:
        print(f"Resuming from checkpoint at index {start_idx} "
              f"({len(results)} results cached)")

    # Process
    for i in range(start_idx, len(records)):
        rec = records[i]
        print(f"[{i + 1}/{len(records)}] {rec['company_name'][:50]}")
        try:
            result = process_contractor(rec)
            results.append(result)
            if result["email"]:
                print(f"  -> {result['email']} (conf={result['confidence']}, "
                      f"tier={result['tier']})")
            else:
                print(f"  -> no email (conf={result['confidence']})")
        except Exception as exc:
            print(f"  ERROR: {exc}")
            results.append({
                "company_name": rec["company_name"], "email": "",
                "phone": rec.get("phone", ""), "domain": "",
                "confidence": 0.0, "sources_matched": "",
                "trade": rec.get("trade_type", ""),
                "city": rec.get("city", ""),
                "license_number": rec.get("license_number", ""),
                "tier": "tier3_unverified",
            })

        # Checkpoint every 50
        if (i + 1) % 50 == 0:
            save_checkpoint(i + 1, results)
            print(f"  -- checkpoint saved at {i + 1} --")

    # Train model if requested
    if args.train:
        print("\nTraining fingerprint model...")
        train_fingerprint_model(results)

    # Write outputs
    write_csv(results, args.min_confidence)
    write_summary(results)

    # Clean up checkpoint on successful completion
    if CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()
        print("Checkpoint cleared (run complete).")


if __name__ == "__main__":
    main()
