"""
Equalyzer — Progress Dashboard
================================
Run at any time to see what's in the DB:
  python progress.py

Shows:
- Total filings, officers, orgs matched
- Breakdown by year
- Estimated % complete vs IRS total
- Local cache stats
"""

import os, time, requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
ETL_TOKEN    = os.getenv("ETL_SECRET_TOKEN2", "")
INGEST_URL   = f"{SUPABASE_URL}/functions/v1/etl-ingest"
CACHE_FILE   = Path("processed_ids.txt")

# Approximate total 990/990EZ filings across 2022-2024
IRS_TOTAL_ESTIMATE = 578_000


def call_edge(action, data={}):
    try:
        r = requests.post(
            INGEST_URL,
            headers={"Content-Type": "application/json", "x-etl-token": ETL_TOKEN},
            json={"action": action, "data": data},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_counts():
    """Query Supabase directly for table counts via ping."""
    r = call_edge("ping")
    return r.get("counts", {})


def get_filing_count():
    """Get count of ingested filings via object_ids endpoint."""
    total = 0
    offset = 0
    while True:
        r = call_edge("get_object_ids", {"limit": 5000, "offset": offset})
        if not r.get("success"):
            break
        ids = r.get("object_ids", [])
        total += len(ids)
        if len(ids) < 5000:
            break
        offset += 5000
    return total


def cache_stats():
    if not CACHE_FILE.exists():
        return 0
    lines = CACHE_FILE.read_text(encoding="utf-8").splitlines()
    return len([l for l in lines if l.strip()])


def bar(pct, width=30):
    filled = int(width * pct / 100)
    return "[" + "█" * filled + "░" * (width - filled) + f"] {pct:.1f}%"


def run():
    print("\n" + "=" * 55)
    print("  EQUALYZER — ETL Progress Dashboard")
    print("=" * 55)

    if not SUPABASE_URL or not ETL_TOKEN:
        print("ERROR: Missing SUPABASE_URL or ETL_SECRET_TOKEN2 in .env")
        return

    print("\nConnecting to DB...")
    counts = get_counts()

    if not counts:
        print("Could not fetch DB counts (is the edge function deployed?)")
    else:
        print("\n── Database ──────────────────────────────────")
        for table, count in counts.items():
            print(f"  {table:<35} {count:>10,}")

    print("\n── Filings Progress ──────────────────────────")
    print("  Counting ingested filings (may take a moment)...")
    filing_count = get_filing_count()
    pct = min(filing_count / IRS_TOTAL_ESTIMATE * 100, 100)
    print(f"  Ingested : {filing_count:>10,}")
    print(f"  Estimated total (2022-2024): {IRS_TOTAL_ESTIMATE:}")
    print(f"  {bar(pct)}")
    remaining = max(IRS_TOTAL_ESTIMATE - filing_count, 0)
    print(f"  Remaining: ~{remaining:}")

    print("\n── Local Cache ───────────────────────────────")
    cached = cache_stats()
    if cached:
        print(f"  processed_ids.txt: {cached:} entries")
    else:
        print("  No local cache file found yet")

    print("\n── Downloads ─────────────────────────────────")
    dl_dir = Path("downloads/990")
    if dl_dir.exists():
        zips   = list(dl_dir.glob("*.zip"))
        csvs   = list(dl_dir.glob("*.csv"))
        total_mb = sum(f.stat().st_size for f in zips) / 1024 / 1024
        print(f"  ZIP files cached : {len(zips)} ({total_mb:.0f} MB)")
        print(f"  Index CSVs       : {len(csvs)}")
        for z in sorted(zips):
            mb = z.stat().st_size / 1024 / 1024
            print(f"    {z.name:<45} {mb:>7.1f} MB")
    else:
        print("  downloads/990 folder not found yet")

    print("\n" + "=" * 55 + "\n")


if __name__ == "__main__":
    run()
