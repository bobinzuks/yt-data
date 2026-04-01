#!/usr/bin/env python3
"""Scrape BC municipality building permit data to extract contractor contacts.

Usage:
    python scrape_permits.py --city vancouver --trade electrical --limit 500
    python scrape_permits.py --city all --trade all --output data/permit-contacts.csv

Also callable from pipeline:
    from scrape_permits import scrape_permits
    rows = scrape_permits(city="vancouver", trade="plumbing", limit=1000)
"""
import argparse, csv, json, os, re, signal, time, random
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

# -- Constants ---------------------------------------------------------------
REQUEST_TIMEOUT = 15
CHECKPOINT_EVERY = 50
MAX_RETRIES = 3
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
STRIP_SUFFIXES = re.compile(
    r"\b(ltd|ltda|inc|corp|co|llc|limited|incorporated|company)\b", re.I)
BLOCKED_DOMAINS = {
    "example.com", "localhost", "sentry.io", "wix.com",
    "squarespace.com", "wordpress.com", "wordpress.org",
}
RELEVANT_TRADES = {
    "electrical", "plumbing", "hvac", "mechanical", "roofing",
    "painting", "general contractor", "renovation", "demolition",
    "addition", "alteration", "tenant improvement", "new building",
    "fire suppression", "sprinkler", "gas", "heating",
}
CONTACT_PATHS = ["/", "/contact", "/contact-us", "/about"]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# -- Source configs ----------------------------------------------------------
SOURCES = {
    "vancouver": {
        "api": "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/issued-building-permits/records",
        "format": "json",
        "params": {"limit": 100, "offset": 0},
        "contractor_field": "applicant",
        "trade_field": "typeofwork",
        "date_field": "issueyear",
        "address_field": "address",
    },
    "surrey": {
        "api": "https://data.surrey.ca/api/3/action/datastore_search",
        "format": "json",
        "resource_id": "to-be-determined",
        "contractor_field": "applicant",
        "trade_field": "permit_type",
        "date_field": "issue_date",
        "address_field": "address",
    },
}

# -- Graceful shutdown -------------------------------------------------------
shutdown_requested = False

def _handle_signal(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    print("\n[!] Shutdown requested, saving checkpoint...")

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

# -- HTTP helpers ------------------------------------------------------------
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept-Language": "en-CA,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s

def _get(session: requests.Session, url: str, params=None) -> str | None:
    for attempt in range(MAX_RETRIES):
        if shutdown_requested:
            return None
        session.headers["User-Agent"] = random.choice(USER_AGENTS)
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT,
                               allow_redirects=True)
            if resp.status_code in (429, 503):
                wait = (2 ** attempt) + random.uniform(0, 1)
                print(f"  [retry] {resp.status_code}, waiting {wait:.1f}s")
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                return None
            return resp.text
        except requests.RequestException as exc:
            print(f"  [error] {url}: {exc}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
    return None

def _get_json(session: requests.Session, url: str, params=None) -> dict | None:
    text = _get(session, url, params=params)
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None

# -- Permit fetching ---------------------------------------------------------
def _trade_matches(work_type: str, trade_filter: str) -> bool:
    wt = work_type.lower()
    if trade_filter == "all":
        return any(t in wt for t in RELEVANT_TRADES)
    return trade_filter.lower() in wt

def fetch_vancouver(session, trade_filter: str, limit: int) -> list[dict]:
    """Paginate Vancouver open data API for permit records."""
    records, cfg, offset = [], SOURCES["vancouver"], 0
    print(f"[vancouver] Fetching permits (trade={trade_filter}, limit={limit})...")
    while len(records) < limit:
        if shutdown_requested:
            break
        data = _get_json(session, cfg["api"], params={"limit": 100, "offset": offset})
        if not data:
            break
        results = data.get("results", [])
        if not results:
            break
        for r in results:
            applicant = (r.get(cfg["contractor_field"]) or "").strip()
            work_type = (r.get(cfg["trade_field"]) or "").strip()
            if not applicant or not work_type or not _trade_matches(work_type, trade_filter):
                continue
            records.append({
                "company_raw": applicant,
                "trade": work_type,
                "year": str(r.get(cfg["date_field"]) or ""),
                "address": (r.get(cfg["address_field"]) or "").strip(),
                "city": "vancouver",
            })
            if len(records) >= limit:
                break
        offset += 100
        print(f"  [vancouver] {len(records)} records (offset={offset})")
        time.sleep(1)
    print(f"[vancouver] Total raw records: {len(records)}")
    return records

def fetch_surrey(session, trade_filter: str, limit: int) -> list[dict]:
    """Placeholder for Surrey open data (resource_id TBD)."""
    print("[surrey] Source not yet configured (resource_id needed). Skipping.")
    return []

FETCHERS = {"vancouver": fetch_vancouver, "surrey": fetch_surrey}

# -- Company normalization ---------------------------------------------------
def _normalize_company(name: str) -> str:
    name = STRIP_SUFFIXES.sub("", name.lower().strip())
    name = re.sub(r"[^a-z0-9 ]", "", name)
    return re.sub(r"\s+", "", name).strip()

def aggregate_permits(records: list[dict]) -> dict[str, dict]:
    """Group raw permit records by normalized company name."""
    companies: dict[str, dict] = {}
    for r in records:
        key = _normalize_company(r["company_raw"])
        if len(key) < 3:
            continue
        if key not in companies:
            companies[key] = {
                "company_name": r["company_raw"], "norm": key,
                "trade": r["trade"], "city": r["city"],
                "permit_count": 0, "years": [],
            }
        companies[key]["permit_count"] += 1
        if r["year"]:
            companies[key]["years"].append(r["year"])
    return companies

# -- Domain guessing + email discovery ---------------------------------------
def _guess_domains(norm: str, raw: str) -> list[str]:
    candidates = [norm + s for s in (".ca", ".com")]
    full = re.sub(r"[^a-z0-9]", "", raw.lower())
    if full != norm:
        candidates += [full + s for s in (".ca", ".com")]
    return list(dict.fromkeys(candidates))

def _extract_emails(html: str) -> set[str]:
    found = set()
    for m in EMAIL_RE.findall(html):
        email = m.lower().strip()
        local, dom = email.rsplit("@", 1)
        if dom in BLOCKED_DOMAINS or local in {"noreply", "no-reply", "donotreply", "mailer-daemon"}:
            continue
        found.add(email)
    return found

def discover_email(session: requests.Session, norm: str, raw: str) -> tuple[str, str, float]:
    """Try candidate domains to find a contact email. Returns (domain, email, confidence)."""
    for domain in _guess_domains(norm, raw):
        if shutdown_requested:
            break
        for path in CONTACT_PATHS[:3]:
            html = _get(session, f"https://{domain}{path}")
            if not html:
                continue
            emails = _extract_emails(html)
            if emails:
                conf = 0.9 if domain.split(".")[0] == norm else 0.6
                return domain, sorted(emails)[0], conf
            time.sleep(1)
    return "", "", 0.0

# -- Scoring -----------------------------------------------------------------
def _recency_score(years: list[str], current_year: int) -> float:
    if not years:
        return 0.0
    try:
        latest = max(int(y) for y in years if y.isdigit())
    except ValueError:
        return 0.0
    diff = current_year - latest
    if diff <= 0:
        return 1.0
    return round(max(0.0, 1.0 - diff / 10.0), 2)

# -- Checkpoint --------------------------------------------------------------
def _cp_path(output_dir: str) -> str:
    return os.path.join(output_dir, ".checkpoint_permits.json")

def _load_checkpoint(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"processed": [], "rows": []}

def _save_checkpoint(path: str, data: dict):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)

# -- Output ------------------------------------------------------------------
CSV_COLUMNS = [
    "company_name", "domain", "email", "trade", "city",
    "permit_count", "latest_permit_date", "recency_score", "confidence", "source",
]

def _write_csv(path: str, rows: list[dict]):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

def _write_summary(path: str, rows: list[dict], cities: list[str]):
    by_trade: dict[str, int] = {}
    for r in rows:
        by_trade[r.get("trade", "unknown")] = by_trade.get(r.get("trade", "unknown"), 0) + 1
    summary = {
        "total_companies": len(rows),
        "companies_with_email": sum(1 for r in rows if r.get("email")),
        "cities_scraped": cities,
        "by_trade": by_trade,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(path, "w") as f:
        json.dump(summary, f, indent=2)

# -- Main entry point (standalone + pipeline) --------------------------------
def scrape_permits(city="vancouver", trade="all", output="data/permit-contacts.csv",
                   limit=5000) -> list[dict]:
    """Scrape BC building permits and discover contractor emails."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, "..", ".."))
    if not os.path.isabs(output):
        output = os.path.join(project_root, output)
    output_dir = os.path.dirname(output)
    os.makedirs(output_dir, exist_ok=True)

    session = _session()
    current_year = datetime.now().year
    cities = list(FETCHERS.keys()) if city == "all" else [city]
    cities = [c for c in cities if c in FETCHERS]
    if not cities:
        print(f"[error] Unknown city: {city}")
        return []

    # Stage 1: fetch permit records from open data APIs
    all_records = []
    for c in cities:
        if shutdown_requested:
            break
        all_records.extend(FETCHERS[c](session, trade, limit))
    if not all_records:
        print("[done] No permit records found.")
        return []

    # Stage 2: aggregate by company
    companies = aggregate_permits(all_records)
    print(f"\n[aggregate] {len(companies)} unique contractors from {len(all_records)} records")

    # Stage 3: discover emails with checkpoint resume
    cp = _cp_path(output_dir)
    checkpoint = _load_checkpoint(cp)
    processed = set(checkpoint["processed"])
    all_rows: list[dict] = list(checkpoint["rows"])
    queue = [(k, v) for k, v in companies.items() if k not in processed]
    print(f"[email] Discovering emails for {len(queue)} companies "
          f"({len(processed)} already done)...\n")

    count_since_save = 0
    for i, (norm, info) in enumerate(queue):
        if shutdown_requested:
            break
        print(f"  [{i+1}/{len(queue)}] {info['company_name']}", end="")
        domain, email, confidence = discover_email(session, norm, info["company_name"])
        latest_year = ""
        if info["years"]:
            try:
                latest_year = str(max(int(y) for y in info["years"] if y.isdigit()))
            except ValueError:
                pass
        all_rows.append({
            "company_name": info["company_name"], "domain": domain, "email": email,
            "trade": info["trade"], "city": info["city"],
            "permit_count": info["permit_count"], "latest_permit_date": latest_year,
            "recency_score": _recency_score(info["years"], current_year),
            "confidence": confidence, "source": f"permits-{info['city']}",
        })
        processed.add(norm)
        count_since_save += 1
        print(f" -> {email} ({domain})" if email else " -> no email found")
        if count_since_save >= CHECKPOINT_EVERY:
            checkpoint.update({"processed": list(processed), "rows": all_rows})
            _save_checkpoint(cp, checkpoint)
            count_since_save = 0
            print(f"  [checkpoint] {len(all_rows)} rows saved")

    # Final save
    checkpoint.update({"processed": list(processed), "rows": all_rows})
    _save_checkpoint(cp, checkpoint)
    _write_csv(output, all_rows)
    summary_path = output.replace(".csv", "") + "-summary.json"
    _write_summary(summary_path, all_rows, cities)

    with_email = sum(1 for r in all_rows if r.get("email"))
    print(f"\n=== Done ===")
    print(f"  Companies   : {len(all_rows)}")
    print(f"  With email  : {with_email}")
    print(f"  CSV         : {output}")
    print(f"  Summary     : {summary_path}")

    # Clean checkpoint on full completion
    if all(k in processed for k, _ in queue) and not shutdown_requested:
        try:
            os.remove(cp)
        except OSError:
            pass
    return all_rows

# -- CLI ---------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Scrape BC building permits for contractor contacts.")
    ap.add_argument("--city", default="vancouver",
                    help="vancouver, surrey, or all (default: vancouver)")
    ap.add_argument("--trade", default="all",
                    help="electrical, plumbing, hvac, roofing, painting, or all")
    ap.add_argument("--output", default="data/permit-contacts.csv",
                    help="Output CSV path (default: data/permit-contacts.csv)")
    ap.add_argument("--limit", type=int, default=5000,
                    help="Max permit records to fetch (default: 5000)")
    args = ap.parse_args()
    scrape_permits(city=args.city, trade=args.trade, output=args.output, limit=args.limit)

if __name__ == "__main__":
    main()
