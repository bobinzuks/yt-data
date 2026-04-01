#!/usr/bin/env python3
"""SMTP email verification script.

Reads a CSV of scraped emails, verifies each via SMTP RCPT TO,
and outputs results to data/verified-emails.csv with crash recovery.
"""
import asyncio, csv, json, os, random, sys, time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

try:
    import dns.resolver
except ImportError:
    sys.exit("dnspython required: pip install dnspython")

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
OUTPUT_CSV = DATA_DIR / "verified-emails.csv"
CHECKPOINT_FILE = DATA_DIR / ".verify_checkpoint.json"
SUMMARY_FILE = DATA_DIR / "verification-summary.json"

MAX_CONCURRENT, MAX_PER_HOST, CONNECT_TIMEOUT = 5, 2, 8
SKIP_MX_PATTERNS = ["protection.outlook.com"]
CHECKPOINT_INTERVAL = 50
COLS = ["email", "domain", "trade", "city", "status", "mx_host", "response_code", "verified_at"]


def resolve_mx(domain: str) -> str | None:
    try:
        answers = dns.resolver.resolve(domain, "MX")
        records = sorted(answers, key=lambda r: r.preference)
        return str(records[0].exchange).rstrip(".") if records else None
    except Exception:
        return None


def classify_code(code: int) -> str:
    if code == 250:
        return "verified"
    if code in (550, 551, 552, 553):
        return "invalid"
    if code in (421, 450, 451, 452):
        return "temp_fail"
    return "invalid"


async def smtp_verify(email: str, mx_host: str) -> tuple[str, int]:
    """Connect to mx_host:25, verify email via RCPT TO. Returns (status, code)."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(mx_host, 25), timeout=CONNECT_TIMEOUT
        )
    except (asyncio.TimeoutError, OSError):
        return "timeout", 0
    try:
        await asyncio.wait_for(reader.readline(), timeout=CONNECT_TIMEOUT)
        writer.write(b"EHLO check.local\r\n")
        await writer.drain()
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=CONNECT_TIMEOUT)
            if len(line) < 4 or line[3:4] == b" ":
                break
        writer.write(f"MAIL FROM:<verify@check.local>\r\n".encode())
        await writer.drain()
        await asyncio.wait_for(reader.readline(), timeout=CONNECT_TIMEOUT)
        writer.write(f"RCPT TO:<{email}>\r\n".encode())
        await writer.drain()
        resp = await asyncio.wait_for(reader.readline(), timeout=CONNECT_TIMEOUT)
        code = int(resp[:3])
        writer.write(b"QUIT\r\n")
        await writer.drain()
        try:
            await asyncio.wait_for(reader.readline(), timeout=2)
        except Exception:
            pass
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return classify_code(code), code
    except (asyncio.TimeoutError, Exception):
        try:
            writer.close()
        except Exception:
            pass
        return "timeout", 0


def load_checkpoint() -> set[str]:
    if CHECKPOINT_FILE.exists():
        return set(json.loads(CHECKPOINT_FILE.read_text()).get("verified_emails", []))
    return set()


def save_checkpoint(done: set[str]) -> None:
    CHECKPOINT_FILE.write_text(json.dumps({"verified_emails": sorted(done)}))


def load_existing_verified() -> set[str]:
    emails = set()
    if OUTPUT_CSV.exists():
        with open(OUTPUT_CSV, newline="") as f:
            for row in csv.DictReader(f):
                emails.add(row.get("email", ""))
    return emails


def append_results(results: list[dict]) -> None:
    exists = OUTPUT_CSV.exists() and OUTPUT_CSV.stat().st_size > 0
    with open(OUTPUT_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        if not exists:
            w.writeheader()
        w.writerows(results)


def load_input_csv(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return [{k: v.strip() for k, v in row.items()} for row in csv.DictReader(f)]


def make_result(email, domain, trade, city, status, mx_host, code) -> dict:
    return dict(email=email, domain=domain, trade=trade, city=city,
                status=status, mx_host=mx_host, response_code=code,
                verified_at=datetime.now(timezone.utc).isoformat())


async def process_batch(rows, already_done, mx_cache) -> dict[str, int]:
    stats = defaultdict(int)
    global_sem = asyncio.Semaphore(MAX_CONCURRENT)
    host_sems: dict[str, asyncio.Semaphore] = defaultdict(lambda: asyncio.Semaphore(MAX_PER_HOST))
    host_last: dict[str, float] = {}
    pending: list[dict] = []
    done = set(already_done)
    lock = asyncio.Lock()

    async def flush_if_needed():
        total = sum(stats.values())
        if total % CHECKPOINT_INTERVAL == 0 and pending:
            append_results(pending)
            pending.clear()
            save_checkpoint(done)

    async def record(result, status_key):
        async with lock:
            pending.append(result)
            done.add(result["email"])
            stats[status_key] += 1
            await flush_if_needed()

    async def verify_one(row: dict) -> None:
        email = row.get("email", "")
        domain = row.get("domain", "") or (email.split("@")[-1] if "@" in email else "")
        trade, city = row.get("trade", ""), row.get("city", "")

        if not email or "@" not in email or email in done:
            stats["skipped"] += 1
            return

        if domain not in mx_cache:
            loop = asyncio.get_event_loop()
            mx_cache[domain] = await loop.run_in_executor(None, resolve_mx, domain)
        mx_host = mx_cache[domain]

        if mx_host is None:
            await record(make_result(email, domain, trade, city, "timeout", "", 0), "timeout")
            return

        if any(p in mx_host.lower() for p in SKIP_MX_PATTERNS):
            await record(make_result(email, domain, trade, city, "skipped_mx", mx_host, 0), "skipped")
            return

        async with global_sem, host_sems[mx_host]:
            now = time.monotonic()
            delay = max(0, random.uniform(1, 3) - (now - host_last.get(mx_host, 0)))
            if delay > 0:
                await asyncio.sleep(delay)
            status, code = await smtp_verify(email, mx_host)
            host_last[mx_host] = time.monotonic()

        await record(make_result(email, domain, trade, city, status, mx_host, code), status)

    await asyncio.gather(*[asyncio.create_task(verify_one(r)) for r in rows], return_exceptions=True)

    if pending:
        append_results(pending)
        save_checkpoint(done)
    return dict(stats)


def print_summary(stats, total):
    checked = sum(stats.values())
    print(f"\n--- Verification Summary ---")
    for label, key in [("Total input rows", None), ("Total checked", None),
                       ("Verified (250)", "verified"), ("Invalid", "invalid"),
                       ("Temp fail", "temp_fail"), ("Timeout", "timeout"), ("Skipped", "skipped")]:
        val = total if label == "Total input rows" else checked if label == "Total checked" else stats.get(key, 0)
        print(f"  {label + ':':<19} {val}")
    print(f"  Output: {OUTPUT_CSV}")


def save_summary(stats, total):
    summary = {"total_input": total, "total_checked": sum(stats.values()),
               **{k: stats.get(k, 0) for k in ("verified", "invalid", "temp_fail", "timeout", "skipped")},
               "completed_at": datetime.now(timezone.utc).isoformat()}
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SUMMARY_FILE.write_text(json.dumps(summary, indent=2))
    print(f"  Summary: {SUMMARY_FILE}")


def main():
    if len(sys.argv) < 2:
        sys.exit(f"Usage: {sys.argv[0]} <input.csv>")
    input_path = sys.argv[1]
    if not os.path.isfile(input_path):
        sys.exit(f"File not found: {input_path}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rows = load_input_csv(input_path)
    print(f"Loaded {len(rows)} rows from {input_path}")

    already_done = load_checkpoint() | load_existing_verified()
    if already_done:
        print(f"Resuming: {len(already_done)} emails already processed")

    mx_cache: dict[str, str | None] = {}
    stats = asyncio.run(process_batch(rows, already_done, mx_cache))
    print_summary(stats, len(rows))
    save_summary(stats, len(rows))

    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


if __name__ == "__main__":
    main()
