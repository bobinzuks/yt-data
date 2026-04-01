#!/usr/bin/env python3
"""Scrape business emails for a trade+city combination from YellowPages.ca."""

import argparse
import csv
import json
import os
import re
import signal
import sys
import time
import random
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_REQUESTS_PER_RUN = 100
REQUEST_TIMEOUT = 10
CHECKPOINT_INTERVAL = 10
MAX_RETRIES = 3
YP_PAGES = 20

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

BLOCKED_LOCAL_PARTS = {"noreply", "no-reply", "no_reply", "donotreply", "mailer-daemon"}
BLOCKED_DOMAINS = {
    "example.com", "localhost", "sentry.io", "webador.com", "wix.com",
    "squarespace.com", "wordpress.com", "wordpress.org",
}
FREEMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "yahoo.ca", "outlook.com", "hotmail.com",
    "shaw.ca", "telus.net", "live.com", "msn.com",
}

CONTACT_PATHS = ["/", "/contact", "/contact-us", "/about"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# ---------------------------------------------------------------------------
# Globals for graceful shutdown
# ---------------------------------------------------------------------------

shutdown_requested = False


def _handle_signal(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    print("\n[!] Shutdown requested, finishing current work...")


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class RequestBudget:
    """Track and enforce the per-run request limit."""

    def __init__(self, limit: int = MAX_REQUESTS_PER_RUN):
        self.limit = limit
        self.count = 0

    def exhausted(self) -> bool:
        return self.count >= self.limit

    def increment(self):
        self.count += 1


def _random_delay():
    time.sleep(random.uniform(2, 5))


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept-Language": "en-CA,en;q=0.9",
        "Connection": "keep-alive",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


def _get(session: requests.Session, url: str, budget: RequestBudget) -> str | None:
    """GET with retry, backoff, UA rotation, and budget tracking."""
    if budget.exhausted():
        return None
    for attempt in range(MAX_RETRIES):
        if shutdown_requested:
            return None
        session.headers["User-Agent"] = random.choice(USER_AGENTS)
        try:
            budget.increment()
            resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if resp.status_code in (429, 503):
                wait = (2 ** attempt) + random.uniform(0, 1)
                print(f"  [retry] {resp.status_code} on {url}, waiting {wait:.1f}s")
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


def _extract_emails(html: str, allowed_domain: str | None = None) -> set[str]:
    """Pull emails from raw HTML and apply filters."""
    found = set()
    for match in EMAIL_RE.findall(html):
        email = match.lower().strip()
        local, domain = email.rsplit("@", 1)
        if local in BLOCKED_LOCAL_PARTS:
            continue
        if domain in BLOCKED_DOMAINS:
            continue
        if allowed_domain and domain not in FREEMAIL_DOMAINS:
            if allowed_domain not in domain and domain not in allowed_domain:
                continue
        found.add(email)
    return found


def _extract_domain(url: str) -> str | None:
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = parsed.hostname
        if not host:
            return None
        host = host.lower().removeprefix("www.")
        if host in ("yellowpages.ca", ""):
            return None
        return host
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


def _checkpoint_path(output_dir: str, trade: str, city: str) -> str:
    return os.path.join(output_dir, f".checkpoint_{trade}_{city}.json")


def _load_checkpoint(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"completed_domains": [], "rows": [], "last_yp_page": 0, "domains_queue": []}


def _save_checkpoint(path: str, data: dict):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)

# ---------------------------------------------------------------------------
# Scraping stages
# ---------------------------------------------------------------------------


def scrape_yellowpages(session, trade, city, budget, start_page=1) -> list[str]:
    """Return list of website URLs found on YellowPages.ca listings."""
    domains: list[str] = []
    for page in range(start_page, YP_PAGES + 1):
        if shutdown_requested or budget.exhausted():
            break
        url = f"https://www.yellowpages.ca/search/si/{page}/{trade}/{city}+BC"
        print(f"[YP] page {page}: {url}")
        html = _get(session, url, budget)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        listings = soup.select("div.listing__content")
        if not listings:
            listings = soup.select("div.listing")
        if not listings:
            print(f"  No listings found on page {page}, stopping YP pagination.")
            break
        for listing in listings:
            link = listing.select_one("a.listing__link--website, a[data-analytics='website']")
            if not link:
                continue
            href = link.get("href", "")
            domain = _extract_domain(href)
            if domain and domain not in domains:
                domains.append(domain)
        _random_delay()
    return domains


def scrape_domain_emails(session, domain, budget, trade, city) -> list[dict]:
    """Scrape a single domain for emails across common contact pages."""
    rows = []
    seen_emails: set[str] = set()
    for path in CONTACT_PATHS:
        if shutdown_requested or budget.exhausted():
            break
        url = f"https://{domain}{path}"
        html = _get(session, url, budget)
        if not html:
            continue
        emails = _extract_emails(html, allowed_domain=domain)
        now = datetime.now(timezone.utc).isoformat()
        for email in emails:
            if email not in seen_emails:
                seen_emails.add(email)
                rows.append({
                    "domain": domain,
                    "email": email,
                    "trade": trade,
                    "city": city,
                    "source_url": url,
                    "scraped_at": now,
                })
        _random_delay()
    return rows

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

CSV_COLUMNS = ["domain", "email", "trade", "city", "source_url", "scraped_at"]


def _write_csv(path: str, rows: list[dict]):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _write_summary(path: str, rows: list[dict], trade: str, city: str, budget: RequestBudget):
    unique_emails = {r["email"] for r in rows}
    unique_domains = {r["domain"] for r in rows}
    summary = {
        "trade": trade,
        "city": city,
        "total_emails": len(unique_emails),
        "total_domains_with_emails": len(unique_domains),
        "total_rows": len(rows),
        "requests_used": budget.count,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(path, "w") as f:
        json.dump(summary, f, indent=2)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Scrape business emails for a trade+city.")
    parser.add_argument("--trade", required=True, help="Trade to search (e.g. painters)")
    parser.add_argument("--city", required=True, help="City to search (e.g. Vancouver)")
    parser.add_argument("--output", default=None, help="Output CSV path")
    parser.add_argument("--output-dir", default="results/", help="Output directory")
    parser.add_argument("--user-agent", default=None, help="Primary user agent")
    parser.add_argument("--delay-min", type=float, default=2.0, help="Min delay between requests")
    parser.add_argument("--delay-max", type=float, default=5.0, help="Max delay between requests")
    args = parser.parse_args()

    trade = args.trade.strip()
    city = args.city.strip()
    output_dir = args.output_dir

    # Apply CLI overrides
    if args.user_agent:
        USER_AGENTS.insert(0, args.user_agent)
    global _random_delay
    _delay_min = args.delay_min
    _delay_max = args.delay_max
    def _random_delay():
        time.sleep(random.uniform(_delay_min, _delay_max))

    os.makedirs(output_dir, exist_ok=True)

    cp_path = _checkpoint_path(output_dir, trade, city)
    checkpoint = _load_checkpoint(cp_path)
    budget = RequestBudget()
    session = _session()

    completed = set(checkpoint["completed_domains"])
    all_rows: list[dict] = list(checkpoint["rows"])
    domains_queue: list[str] = list(checkpoint["domains_queue"])

    # Stage 1: YellowPages scraping (resume aware)
    if not domains_queue and checkpoint["last_yp_page"] < YP_PAGES:
        start = checkpoint["last_yp_page"] + 1
        print(f"=== Stage 1: YellowPages (starting page {start}) ===")
        domains_queue = scrape_yellowpages(session, trade, city, budget, start_page=start)
        checkpoint["domains_queue"] = domains_queue
        checkpoint["last_yp_page"] = YP_PAGES
        _save_checkpoint(cp_path, checkpoint)

    # Stage 2: Scrape each domain
    print(f"\n=== Stage 2: Scraping {len(domains_queue)} domains ===")
    processed_since_save = 0
    for i, domain in enumerate(domains_queue):
        if shutdown_requested or budget.exhausted():
            print(f"[stop] shutdown={shutdown_requested} budget_exhausted={budget.exhausted()}")
            break
        if domain in completed:
            continue
        print(f"[{i+1}/{len(domains_queue)}] {domain}")
        rows = scrape_domain_emails(session, domain, budget, trade, city)
        all_rows.extend(rows)
        completed.add(domain)
        processed_since_save += 1
        if processed_since_save >= CHECKPOINT_INTERVAL:
            checkpoint["completed_domains"] = list(completed)
            checkpoint["rows"] = all_rows
            _save_checkpoint(cp_path, checkpoint)
            processed_since_save = 0
            print(f"  [checkpoint] saved ({len(all_rows)} rows, {len(completed)} domains)")

    # Final save
    checkpoint["completed_domains"] = list(completed)
    checkpoint["rows"] = all_rows
    _save_checkpoint(cp_path, checkpoint)

    # Write outputs
    csv_path = args.output or os.path.join(output_dir, f"{trade}_{city}.csv")
    summary_path = csv_path.replace(".csv", "_summary.json")
    _write_csv(csv_path, all_rows)
    _write_summary(summary_path, all_rows, trade, city, budget)

    print(f"\n=== Done ===")
    print(f"  Emails found : {len({r['email'] for r in all_rows})}")
    print(f"  Domains hit  : {len(completed)}")
    print(f"  Requests used: {budget.count}/{budget.limit}")
    print(f"  CSV          : {csv_path}")
    print(f"  Summary      : {summary_path}")

    # Clean up checkpoint on full completion
    remaining = [d for d in domains_queue if d not in completed]
    if not remaining and not budget.exhausted():
        try:
            os.remove(cp_path)
            print("  Checkpoint removed (run complete).")
        except OSError:
            pass


if __name__ == "__main__":
    main()
