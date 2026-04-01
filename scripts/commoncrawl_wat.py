#!/usr/bin/env python3
"""Common Crawl WAT processor — builds GA_ID -> domain[] graph for .ca businesses.

Two modes: CDX index API (fast, targeted) or full WAT file stream (comprehensive).
Uses only stdlib. Output: ga_id->domains JSON, domains CSV, summary stats.
"""

import argparse
import csv
import gzip
import io
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from collections import defaultdict
from pathlib import Path

BASE_URL = "https://data.commoncrawl.org"
CDX_URL = "https://index.commoncrawl.org"
COLLINFO_URL = f"{CDX_URL}/collinfo.json"
CHECKPOINT_FILE = "data/cc-checkpoint.json"

# Regex patterns for tracking IDs
RE_GA = re.compile(r"UA-\d{4,10}-\d{1,4}")
RE_GA4 = re.compile(r"G-[A-Z0-9]{10,12}")
RE_GTM = re.compile(r"GTM-[A-Z0-9]{5,8}")
RE_PIXEL = re.compile(r"fbq\(['\"]init['\"],\s*['\"](\d{15,16})['\"]")
RE_EMAIL = re.compile(r"mailto:([^\"'<>\s]+)")
RE_PHONE = re.compile(r"tel:(\+?1?[\d\-().]{10,})")

# Canadian trade signals in meta keywords/description
TRADE_KEYWORDS = [
    "contractor", "plumber", "electrician", "hvac", "roofing", "landscaping",
    "renovation", "construction", "painting", "flooring", "welding",
    "carpentry", "masonry", "drywall", "insulation", "excavation",
]


def get_latest_crawl():
    """Fetch the latest crawl ID from Common Crawl."""
    try:
        req = urllib.request.Request(COLLINFO_URL)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            return data[0]["id"]  # most recent is first
    except Exception:
        return "CC-MAIN-2024-51"


def fetch_cdx_page(crawl, page, limit=5000):
    """Fetch one page of .ca URLs from the CDX index API."""
    url = (
        f"{CDX_URL}/{crawl}-index?url=*.ca&output=json"
        f"&limit={limit}&page={page}"
    )
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "email-velocity-bot/1.0")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            lines = resp.read().decode(errors="replace").strip().split("\n")
            return [json.loads(line) for line in lines if line.strip()]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise
    except Exception as e:
        print(f"  CDX page {page} error: {e}", file=sys.stderr)
        return []


def get_wat_paths(crawl):
    """Download and parse the WAT paths list for a crawl."""
    url = f"{BASE_URL}/crawl-data/{crawl}/wat.paths.gz"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "email-velocity-bot/1.0")
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = gzip.decompress(resp.read())
        return raw.decode().strip().split("\n")


def stream_wat_file(crawl, wat_index, paths):
    """Stream and decompress a single WAT file from S3."""
    path = paths[wat_index]
    url = f"{BASE_URL}/{path}"
    print(f"Streaming WAT: {url}", file=sys.stderr)
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "email-velocity-bot/1.0")
    resp = urllib.request.urlopen(req, timeout=120)
    return gzip.GzipFile(fileobj=io.BytesIO(resp.read()))


def parse_warc_records(stream):
    """Parse WARC-format WAT records properly."""
    content = stream.read().decode(errors="replace")
    records = content.split("WARC/1.0\r\n")
    if len(records) <= 1:
        records = content.split("WARC/1.0\n")
    for record in records:
        if not record.strip():
            continue
        json_start = record.find("{")
        if json_start == -1:
            continue
        # Find the matching closing brace — take the largest valid JSON
        json_text = record[json_start:]
        try:
            payload = json.loads(json_text)
        except json.JSONDecodeError:
            # Try line-by-line — each WARC record has one JSON blob
            for i, ch in enumerate(json_text):
                if ch == "\n" and i > 100:
                    try:
                        payload = json.loads(json_text[:i])
                        break
                    except json.JSONDecodeError:
                        continue
            else:
                continue
        url = ""
        try:
            url = payload["Envelope"]["WARC-Header-Metadata"]["WARC-Target-URI"]
        except (KeyError, TypeError):
            pass
        if url:
            yield url, payload


def is_ca_domain(url):
    """Check if URL belongs to a .ca domain."""
    try:
        host = url.split("//")[-1].split("/")[0].split(":")[0].lower()
        return host.endswith(".ca")
    except Exception:
        return False


def extract_signals(url, payload):
    """Extract GA/GTM/Pixel IDs and contact info from a WAT JSON envelope."""
    result = {
        "domain": url.split("//")[-1].split("/")[0].split(":")[0].lower(),
        "ga_ids": set(), "gtm_ids": set(), "pixel_ids": set(),
        "emails": set(), "phones": set(), "trade_signals": [],
    }
    try:
        http_meta = payload["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]
    except (KeyError, TypeError):
        return result

    html_meta = http_meta.get("HTML-Metadata", {})
    head = html_meta.get("Head", {})

    # Scripts — src attrs and inline content
    for script in head.get("Scripts", []):
        src = script.get("src", "")
        text = script.get("text", "") or json.dumps(script)
        combined = f"{src} {text}"
        result["ga_ids"].update(RE_GA.findall(combined))
        result["ga_ids"].update(RE_GA4.findall(combined))
        result["gtm_ids"].update(RE_GTM.findall(combined))
        for m in RE_PIXEL.findall(combined):
            result["pixel_ids"].add(m)

    # Metas — description, keywords
    for meta in head.get("Metas", []):
        content = meta.get("content", "")
        name = meta.get("name", "").lower()
        if name in ("description", "keywords"):
            content_lower = content.lower()
            for kw in TRADE_KEYWORDS:
                if kw in content_lower:
                    result["trade_signals"].append(kw)

    # Links — mailto and tel
    for link in head.get("Links", []):
        href = link.get("url", "") or link.get("href", "")
        for m in RE_EMAIL.findall(href):
            result["emails"].add(m)
        for m in RE_PHONE.findall(href):
            result["phones"].add(m)

    # Also scan response headers
    headers = http_meta.get("Headers", {})
    raw_headers = json.dumps(headers)
    result["ga_ids"].update(RE_GA.findall(raw_headers))
    result["gtm_ids"].update(RE_GTM.findall(raw_headers))

    return result


def load_checkpoint(output_dir):
    """Load processing checkpoint."""
    cp_path = os.path.join(output_dir, "cc-checkpoint.json")
    if os.path.exists(cp_path):
        with open(cp_path) as f:
            return json.load(f)
    return {"processed": 0, "wat_index": 0, "cdx_page": 0}


def save_checkpoint(output_dir, cp):
    """Save processing checkpoint."""
    cp_path = os.path.join(output_dir, "cc-checkpoint.json")
    with open(cp_path, "w") as f:
        json.dump(cp, f)


def run_cdx_mode(crawl, limit, output_dir, resume):
    """Strategy 1: Use CDX index API to find .ca URLs then extract signals."""
    ga_index = defaultdict(set)
    domains = {}
    cp = load_checkpoint(output_dir) if resume else {"processed": 0, "cdx_page": 0}
    page = cp.get("cdx_page", 0)
    total = cp.get("processed", 0)

    print(f"CDX mode: crawl={crawl}, starting page={page}", file=sys.stderr)
    empty_pages = 0

    while total < limit:
        print(f"  Fetching CDX page {page} (total so far: {total})...", file=sys.stderr)
        records = fetch_cdx_page(crawl, page, limit=min(5000, limit - total))
        if not records:
            empty_pages += 1
            if empty_pages >= 3:
                print("  3 consecutive empty pages, stopping.", file=sys.stderr)
                break
            page += 1
            continue
        empty_pages = 0

        for rec in records:
            url = rec.get("url", "")
            if not is_ca_domain(url):
                continue
            domain = url.split("//")[-1].split("/")[0].split(":")[0].lower()
            if domain not in domains:
                domains[domain] = {
                    "domain": domain, "ga_id": "", "gtm_id": "",
                    "pixel_id": "", "has_email": False, "has_phone": False,
                    "trade_signals": "",
                }
            total += 1
            if total >= limit:
                break

        page += 1
        cp.update({"processed": total, "cdx_page": page})
        if total % 5000 == 0:
            save_checkpoint(output_dir, cp)
            print(f"  Checkpoint at {total} records", file=sys.stderr)
        time.sleep(0.5)  # rate limit

    return ga_index, domains, {"mode": "cdx", "total_domains": len(domains), "total_records": total}


def run_wat_mode(crawl, wat_index, ca_only, limit, output_dir, resume):
    """Strategy 3: Stream full WAT file and extract signals."""
    ga_index = defaultdict(set)
    domains = {}
    cp = load_checkpoint(output_dir) if resume else {"processed": 0, "wat_index": wat_index}

    print(f"WAT mode: crawl={crawl}, index={wat_index}", file=sys.stderr)
    print("Fetching WAT paths list...", file=sys.stderr)
    paths = get_wat_paths(crawl)
    print(f"  Total WAT files: {len(paths)}", file=sys.stderr)

    if wat_index >= len(paths):
        print(f"  WAT index {wat_index} out of range (max {len(paths)-1})", file=sys.stderr)
        sys.exit(1)

    print(f"Downloading WAT file {wat_index}...", file=sys.stderr)
    stream = stream_wat_file(crawl, wat_index, paths)
    total = cp.get("processed", 0)

    for url, payload in parse_warc_records(stream):
        if ca_only and not is_ca_domain(url):
            continue
        signals = extract_signals(url, payload)
        domain = signals["domain"]

        # Update GA index
        for ga_id in signals["ga_ids"]:
            ga_index[ga_id].add(domain)
        for ga_id in signals["gtm_ids"]:
            ga_index[ga_id].add(domain)

        # Update domain record
        if domain not in domains:
            domains[domain] = {
                "domain": domain, "ga_id": "", "gtm_id": "",
                "pixel_id": "", "has_email": False, "has_phone": False,
                "trade_signals": "",
            }
        d = domains[domain]
        if signals["ga_ids"]:
            d["ga_id"] = ",".join(signals["ga_ids"])
        if signals["gtm_ids"]:
            d["gtm_id"] = ",".join(signals["gtm_ids"])
        if signals["pixel_ids"]:
            d["pixel_id"] = ",".join(signals["pixel_ids"])
        if signals["emails"]:
            d["has_email"] = True
        if signals["phones"]:
            d["has_phone"] = True
        if signals["trade_signals"]:
            existing = set(d["trade_signals"].split(",")) if d["trade_signals"] else set()
            existing.update(signals["trade_signals"])
            existing.discard("")
            d["trade_signals"] = ",".join(sorted(existing))

        total += 1
        if total % 1000 == 0:
            cp.update({"processed": total, "wat_index": wat_index})
            save_checkpoint(output_dir, cp)
            print(f"  Processed {total} records, {len(domains)} domains, "
                  f"{len(ga_index)} tracking IDs", file=sys.stderr)
        if total >= limit:
            break

    stats = {
        "mode": "wat", "wat_index": wat_index, "total_records": total,
        "total_domains": len(domains), "unique_ga_ids": len(ga_index),
    }
    return ga_index, domains, stats


def write_outputs(ga_index, domains, stats, output_dir):
    """Write all output files."""
    os.makedirs(output_dir, exist_ok=True)

    # GA index — convert sets to lists for JSON
    ga_path = os.path.join(output_dir, "cc-ga-index.json")
    ga_serializable = {k: sorted(v) for k, v in ga_index.items()}
    with open(ga_path, "w") as f:
        json.dump(ga_serializable, f, indent=2)
    print(f"Wrote {len(ga_index)} tracking IDs to {ga_path}", file=sys.stderr)

    # Domains CSV
    csv_path = os.path.join(output_dir, "cc-domains-ca.csv")
    fields = ["domain", "ga_id", "gtm_id", "pixel_id", "has_email", "has_phone", "trade_signals"]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for d in sorted(domains.values(), key=lambda x: x["domain"]):
            writer.writerow(d)
    print(f"Wrote {len(domains)} domains to {csv_path}", file=sys.stderr)

    # Summary
    summary_path = os.path.join(output_dir, "cc-summary.json")
    stats["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(summary_path, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"Summary: {json.dumps(stats, indent=2)}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Common Crawl WAT processor for Canadian business GA/GTM mapping"
    )
    parser.add_argument("--crawl", default=None, help="Crawl ID (e.g. CC-MAIN-2024-51)")
    parser.add_argument("--mode", choices=["cdx", "wat"], default="cdx",
                        help="Strategy: cdx (index API) or wat (full file stream)")
    parser.add_argument("--wat-index", type=int, default=0,
                        help="WAT file index for wat mode (0-89999)")
    parser.add_argument("--ca-only", action="store_true", default=True,
                        help="Filter to .ca domains only (default: true)")
    parser.add_argument("--no-ca-only", action="store_false", dest="ca_only")
    parser.add_argument("--limit", type=int, default=50000,
                        help="Max records to process")
    parser.add_argument("--output", default=None, help="Output directory")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    project_dir = script_dir.parent
    output_dir = args.output or str(project_dir / "data")

    crawl = args.crawl
    if not crawl:
        print("Detecting latest crawl...", file=sys.stderr)
        crawl = get_latest_crawl()
        print(f"Using crawl: {crawl}", file=sys.stderr)

    if args.mode == "cdx":
        ga_index, domains, stats = run_cdx_mode(crawl, args.limit, output_dir, args.resume)
    else:
        ga_index, domains, stats = run_wat_mode(
            crawl, args.wat_index, args.ca_only, args.limit, output_dir, args.resume
        )

    write_outputs(ga_index, domains, stats, output_dir)
    print(f"\nDone. {stats['total_domains']} domains, "
          f"{stats.get('unique_ga_ids', len(ga_index))} tracking IDs.", file=sys.stderr)


if __name__ == "__main__":
    main()
