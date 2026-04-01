#!/usr/bin/env python3
"""Agent 11 -- RDAP Free Domain -> Email Lookup.
Takes company names from permit/registry CSVs and discovers emails via
the free RDAP protocol (no API key required).
"""
import argparse, csv, json, os, re, sys, time, unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

BASE = Path(__file__).resolve().parent.parent / "data"
CSV_FILES = [
    "ontario-registries.csv", "bc-registries.csv",
    "vancouver-all-permits.csv", "newwest-permits.csv",
    "victoria-permit-emails.csv",
]
RDAP_ENDPOINTS = {
    "ca": "https://rdap.ca/domain/{}",
    "com": "https://rdap.verisign.com/com/v1/domain/{}",
}
STRIP_SUFFIXES = re.compile(
    r"\b(ltd|inc|corp|co|llc|llp|dba|limited|incorporated|corporation"
    r"|company|enterprises|enterprise|services|service|group|holdings"
    r"|solutions|consulting|contracting)\b\.?", re.I
)
CHECKPOINT_PATH = BASE / ".rdap_checkpoint.json"
CHECKPOINT_INTERVAL = 50

# ── CSV column mapping per file ──────────────────────────────────────
COLUMN_MAP = {
    "ontario-registries.csv":       {"company": "company_name", "trade": "trade_type",  "city": "city",    "province": "province"},
    "bc-registries.csv":            {"company": "company_name", "trade": "trade",       "city": "city",    "province": "province"},
    "vancouver-all-permits.csv":    {"company": "company",      "trade": "trade_type",  "city": None,      "province": None},
    "newwest-permits.csv":          {"company": "company",      "trade": "permit_type", "city": None,      "province": None},
    "victoria-permit-emails.csv":   {"company": "company",      "trade": "permit_type", "city": None,      "province": None},
}
CITY_DEFAULTS = {
    "vancouver-all-permits.csv": "Vancouver",
    "newwest-permits.csv": "New Westminster",
    "victoria-permit-emails.csv": "Victoria",
}
PROV_DEFAULTS = {
    "vancouver-all-permits.csv": "BC",
    "newwest-permits.csv": "BC",
    "victoria-permit-emails.csv": "BC",
}

# ── Helpers ──────────────────────────────────────────────────────────

def clean_name(name: str) -> str:
    """Remove legal suffixes and normalize."""
    name = STRIP_SUFFIXES.sub("", name)
    name = re.sub(r"[^\w\s-]", "", name)       # drop special chars
    return " ".join(name.split()).strip()

def slugify(text: str, sep: str = "") -> str:
    """Lowercase ASCII slug; sep='' for compact, sep='-' for hyphenated."""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^\w\s-]", "", text.lower())
    return re.sub(r"[\s_]+", sep, text).strip(sep)

def domain_candidates(company: str, city: str = "") -> list[str]:
    """Generate up to 6 domain candidates from a company name."""
    clean = clean_name(company)
    compact = slugify(clean, "")
    hyphen = slugify(clean, "-")
    city_slug = slugify(city, "") if city else ""
    candidates = []
    seen = set()
    def _add(d):
        if d not in seen and len(candidates) < 6:
            seen.add(d); candidates.append(d)
    _add(f"{compact}.ca")
    _add(f"{compact}.com")
    _add(f"{hyphen}.ca")
    if city_slug:
        _add(f"{compact}-{city_slug}.ca")
    _add(f"{compact}ltd.ca")
    _add(f"{hyphen}.com")
    return candidates

def rdap_fetch(domain: str, retries: int = 3) -> dict | None:
    """Query RDAP for a domain. Returns parsed JSON or None."""
    tld = domain.rsplit(".", 1)[-1]
    url_tpl = RDAP_ENDPOINTS.get(tld)
    if not url_tpl:
        return None
    url = url_tpl.format(domain)
    req = Request(url, headers={"Accept": "application/rdap+json"})
    delay = 1
    for attempt in range(retries):
        try:
            with urlopen(req, timeout=10) as resp:
                return json.loads(resp.read())
        except HTTPError as e:
            if e.code == 404:
                return None
            if e.code == 429:
                time.sleep(delay)
                delay *= 2
                continue
            return None
        except (URLError, OSError, json.JSONDecodeError):
            return None
    return None

def extract_vcard_email(data: dict) -> tuple[str, str]:
    """Walk RDAP JSON to find registrant email and fn name."""
    email, fn = "", ""
    def _parse_vcard(vcard_array):
        nonlocal email, fn
        if not isinstance(vcard_array, list) or len(vcard_array) < 2:
            return
        for entry in vcard_array[1]:
            if not isinstance(entry, list) or len(entry) < 4:
                continue
            prop = entry[0].lower()
            val = entry[3]
            if prop == "email" and not email:
                email = val if isinstance(val, str) else str(val)
            if prop == "fn" and not fn:
                fn = val if isinstance(val, str) else str(val)
    # Top-level vcardArray
    if "vcardArray" in data:
        _parse_vcard(data["vcardArray"])
    # Nested entities
    for ent in data.get("entities", []):
        if "vcardArray" in ent:
            _parse_vcard(ent["vcardArray"])
        for sub in ent.get("entities", []):
            if "vcardArray" in sub:
                _parse_vcard(sub["vcardArray"])
    return email, fn

def match_score(company: str, registrant: str) -> float:
    if not registrant:
        return 0.0
    a = clean_name(company).lower()
    b = clean_name(registrant).lower()
    return SequenceMatcher(None, a, b).ratio()

def tier_label(score: float) -> str:
    if score >= 0.80:
        return "tier1"
    if score >= 0.60:
        return "tier2"
    return "tier3"

# ── Load companies from CSVs ────────────────────────────────────────

def load_companies(input_path: str | None, scan_all: bool, province: str | None) -> list[dict]:
    """Return list of dicts: company_name, trade, city, province."""
    files = []
    if input_path:
        files.append(Path(input_path))
    elif scan_all:
        files = [BASE / f for f in CSV_FILES if (BASE / f).exists()]
    else:
        files = [BASE / f for f in CSV_FILES if (BASE / f).exists()]

    results = []
    seen = set()
    for fpath in files:
        fname = fpath.name
        cmap = COLUMN_MAP.get(fname)
        if not cmap:
            # Fallback: guess columns
            cmap = {"company": "company_name", "trade": "trade", "city": "city", "province": "province"}
        with open(fpath, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get(cmap["company"]) or "").strip()
                if not name or len(name) < 3:
                    continue
                key = name.lower()
                if key in seen:
                    continue
                seen.add(key)
                trade = (row.get(cmap["trade"] or "") or "").strip()
                city = (row.get(cmap["city"] or "") or "").strip() if cmap["city"] else ""
                prov = (row.get(cmap["province"] or "") or "").strip() if cmap["province"] else ""
                city = city or CITY_DEFAULTS.get(fname, "")
                prov = prov or PROV_DEFAULTS.get(fname, "")
                if province and prov.upper() != province.upper():
                    continue
                results.append({"company_name": name, "trade": trade, "city": city, "province": prov})
    return results

def load_existing_emails() -> set[str]:
    """Load emails already found so we can skip those companies."""
    emails = set()
    for fname in ["all-scraped-emails.csv", "verified-emails.csv", "rdap-emails.csv"]:
        p = BASE / fname
        if not p.exists():
            continue
        with open(p, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                e = (row.get("email") or row.get("rdap_email") or "").strip().lower()
                if e:
                    emails.add(e)
    return emails

def load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        with open(CHECKPOINT_PATH) as f:
            return json.load(f)
    return {"processed": [], "results": []}

def save_checkpoint(ckpt: dict):
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump(ckpt, f)

# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="RDAP domain/email lookup from permit data")
    parser.add_argument("--input", help="Specific CSV file to read")
    parser.add_argument("--all", action="store_true", dest="scan_all", help="Scan all data CSVs")
    parser.add_argument("--province", help="Filter by province (BC, ON, AB)")
    parser.add_argument("--offset", type=int, default=0, help="Skip first N companies")
    parser.add_argument("--limit", type=int, default=0, help="Process at most N companies (0=all)")
    parser.add_argument("--output", default=str(BASE / "rdap-emails.csv"))
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--dry-run", action="store_true", help="Show candidates without querying")
    args = parser.parse_args()

    companies = load_companies(args.input, args.scan_all, args.province)
    if not companies:
        print("No companies found. Use --all or --input <file>.", file=sys.stderr)
        sys.exit(1)

    existing_emails = load_existing_emails()
    ckpt = load_checkpoint() if args.resume else {"processed": [], "results": []}
    processed_set = set(ckpt["processed"])

    # Apply offset/limit
    companies = companies[args.offset:]
    if args.limit > 0:
        companies = companies[:args.limit]

    print(f"[rdap] {len(companies)} companies to process (offset={args.offset}, limit={args.limit or 'all'})")
    print(f"[rdap] {len(existing_emails)} existing emails loaded for dedup")
    print(f"[rdap] {len(processed_set)} already processed (checkpoint)")

    stats = {"total_queried": 0, "domains_found": 0, "emails_found": 0,
             "tier1": 0, "tier2": 0, "tier3": 0}
    results = list(ckpt["results"])

    for idx, comp in enumerate(companies):
        name = comp["company_name"]
        if name.lower() in processed_set:
            continue

        candidates = domain_candidates(name, comp["city"])
        if args.dry_run:
            print(f"  {name}: {candidates}")
            continue

        best_email, best_reg, best_domain, best_score = "", "", "", 0.0
        for dom in candidates:
            stats["total_queried"] += 1
            data = rdap_fetch(dom)
            time.sleep(1)  # rate-limit courtesy
            if not data:
                continue
            stats["domains_found"] += 1
            email, reg_name = extract_vcard_email(data)
            if not email:
                continue
            if email.lower() in existing_emails:
                continue
            sc = match_score(name, reg_name)
            if sc > best_score or (not best_email and email):
                best_email, best_reg, best_domain, best_score = email, reg_name, dom, sc
            if sc >= 0.80:
                break  # good enough match, stop trying more

        if best_email:
            t = tier_label(best_score)
            stats["emails_found"] += 1
            stats[t] += 1
            results.append({
                "company_name": name, "domain": best_domain,
                "rdap_email": best_email, "registrant_name": best_reg,
                "match_score": round(best_score, 3), "tier": t,
                "trade": comp["trade"], "city": comp["city"],
                "province": comp["province"],
            })
            existing_emails.add(best_email.lower())
            print(f"  [{t}] {name} -> {best_email} ({best_domain}, score={best_score:.2f})")
        else:
            print(f"  [miss] {name}")

        processed_set.add(name.lower())

        # Checkpoint
        if (idx + 1) % CHECKPOINT_INTERVAL == 0:
            ckpt["processed"] = list(processed_set)
            ckpt["results"] = results
            save_checkpoint(ckpt)
            print(f"  [checkpoint] {idx + 1} processed, {stats['emails_found']} found")

    if args.dry_run:
        return

    # Write output CSV
    out_path = Path(args.output)
    fieldnames = ["company_name", "domain", "rdap_email", "registrant_name",
                   "match_score", "tier", "trade", "city", "province"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(results)
    print(f"\n[rdap] Wrote {len(results)} rows to {out_path}")

    # Write summary JSON
    summary = {
        "total_queried": stats["total_queried"],
        "domains_found": stats["domains_found"],
        "emails_found": stats["emails_found"],
        "tier_breakdown": {
            "tier1_confirmed": stats["tier1"],
            "tier2_probable": stats["tier2"],
            "tier3_weak": stats["tier3"],
        },
        "companies_processed": len(processed_set),
    }
    summary_path = BASE / "rdap-summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"[rdap] Summary -> {summary_path}")

    # Final checkpoint
    ckpt["processed"] = list(processed_set)
    ckpt["results"] = results
    save_checkpoint(ckpt)

    # Clean up checkpoint if fully done
    if args.limit == 0 and args.offset == 0:
        CHECKPOINT_PATH.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
