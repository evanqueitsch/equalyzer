"""
Equalyzer 990 ETL - v3 (Fast Resume Edition)
=============================================
Key improvements over v2:
- Fetches already-ingested object_ids from DB on startup — skips them entirely
- Local cache file (processed_ids.txt) as backup skip-set
- Batch size increased from 10 -> 50
- Progress saved to disk so Ctrl+C and resume works cleanly
- Cleaner ETA/rate display
"""

import os, re, csv, time, hashlib, logging, requests, zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[
        logging.FileHandler("etl_990.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

SUPABASE_URL  = os.getenv("SUPABASE_URL", "").rstrip("/")
ETL_TOKEN     = os.getenv("ETL_SECRET_TOKEN2", "")
MAX_FILINGS   = int(os.getenv("MAX_FILINGS", "100000"))
BATCH_SIZE    = int(os.getenv("BATCH_SIZE", "25"))
INGEST_URL    = f"{SUPABASE_URL}/functions/v1/etl-ingest"
DOWNLOAD_DIR  = Path("downloads/990")
CACHE_FILE    = Path("processed_ids.txt")

IRS_INDEX_CSV = "https://apps.irs.gov/pub/epostcard/990/xml/{year}/index_{year}.csv"
IRS_ZIP_BASE  = "https://apps.irs.gov/pub/epostcard/990/xml/{year}/{year}_TEOS_XML_{part}.zip"

ZIP_PARTS = {
    2024: ["01A","01B","02A","02B","03A","03B","04A","04B",
           "05A","05B","06A","06B","07A","07B","08A","08B",
           "09A","09B","10A","10B","11A","11B","12A","12B"],
    2023: ["01A","01B","02A","02B","03A","03B","04A","04B",
           "05A","05B","06A","06B","07A","07B","08A","08B",
           "09A","09B","10A","10B","11A","11B","12A","12B"],
    2022: ["01A","01B","02A","02B","03A","03B","04A","04B",
           "05A","05B","06A","06B","07A","07B","08A","08B",
           "09A","09B","10A","10B","11A","11B","12A","12B"],
}

NS = {"irs": "http://www.irs.gov/efile"}


# ── Config & connection ───────────────────────────────────────

def check_config():
    if not SUPABASE_URL:
        raise ValueError("Missing SUPABASE_URL in .env")
    if not ETL_TOKEN:
        raise ValueError("Missing ETL_SECRET_TOKEN2 in .env")
    log.info(f"URL : {SUPABASE_URL}")
    log.info(f"Token: {ETL_TOKEN[:8]}...")


def call_edge(action, data, retries=3):
    for attempt in range(retries):
        try:
            r = requests.post(
                INGEST_URL,
                headers={"Content-Type": "application/json", "x-etl-token": ETL_TOKEN},
                json={"action": action, "data": data},
                timeout=120
            )
            if r.status_code == 401:
                raise ValueError("Token rejected — check ETL_SECRET_TOKEN2 in .env")
            r.raise_for_status()
            return r.json()
        except ValueError:
            raise
        except Exception as e:
            log.warning(f"Request error attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(3)
    return {"success": False, "error": "All retries failed"}


def ping():
    log.info("Testing edge function connection...")
    r = call_edge("ping", {})
    if r.get("success"):
        log.info("Connected OK")
        return True
    log.error(f"Connection failed: {r.get('error')}")
    return False


# ── Skip-set: already processed object_ids ────────────────────

def load_skip_set():
    """
    Build the set of object_ids to skip.
    1. Ask the edge function for all ingested object_ids (fast bulk fetch)
    2. Merge with local cache file (covers any gaps)
    """
    skip = set()

    # 1. Fetch from DB via edge function
    log.info("Fetching already-ingested object_ids from DB...")
    offset = 0
    page_size = 5000
    while True:
        r = call_edge("get_object_ids", {"limit": page_size, "offset": offset})
        if not r.get("success"):
            log.warning(f"Could not fetch object_ids from DB: {r.get('error')} — relying on local cache only")
            break
        ids = r.get("object_ids", [])
        skip.update(ids)
        log.info(f"  Fetched {len(skip):,} object_ids from DB so far...")
        if len(ids) < page_size:
            break
        offset += page_size

    log.info(f"DB skip-set: {len(skip):,} already-processed filings")

    # 2. Merge local cache
    if CACHE_FILE.exists():
        cached = set(CACHE_FILE.read_text(encoding="utf-8").splitlines())
        before = len(skip)
        skip.update(cached)
        log.info(f"Local cache added {len(skip) - before:,} more (total skip-set: {len(skip):,})")

    return skip


def save_to_cache(object_ids):
    """Append newly processed IDs to the local cache file."""
    with open(CACHE_FILE, "a", encoding="utf-8") as f:
        for oid in object_ids:
            f.write(oid + "\n")


# ── XML parsing ───────────────────────────────────────────────

def find_text(el, *paths):
    for path in paths:
        try:
            found = el.find(path, NS)
            if found is not None and found.text:
                return found.text.strip()
        except Exception:
            pass
    return None


def find_int(el, *paths):
    v = find_text(el, *paths)
    if v is None:
        return None
    try:
        return int(re.sub(r"[^0-9\-]", "", v))
    except Exception:
        return None


def norm(name):
    if not name:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", name.lower().strip()))


def nhash(name):
    return hashlib.sha256(norm(name).encode()).hexdigest()


def parse_xml(content, object_id):
    try:
        root = ET.fromstring(content)
    except Exception as e:
        log.debug(f"XML parse error {object_id}: {e}")
        return None

    rh = root.find(".//irs:ReturnHeader", NS) or root.find(".//ReturnHeader")
    if rh is None:
        return None

    ein = find_text(rh, "irs:Filer/irs:EIN", "Filer/EIN")
    if not ein:
        return None
    ein = re.sub(r"[^0-9]", "", ein)

    year = find_int(rh, "irs:TaxYr", "TaxYr")
    if year and year > 2100:
        year = int(str(year)[:4])

    form = find_text(rh, "irs:ReturnTypeCd", "ReturnTypeCd") or "990"
    rd = root.find(".//irs:ReturnData", NS) or root.find(".//ReturnData")
    if rd is None:
        return None

    f = (rd.find("irs:IRS990", NS) or rd.find("IRS990") or
         rd.find("irs:IRS990EZ", NS) or rd.find("IRS990EZ"))

    officers = []
    if f is not None:
        for el in (f.findall("irs:Form990PartVIISectionAGrp", NS) or
                   f.findall("Form990PartVIISectionAGrp") or []):
            name = find_text(el, "irs:PersonNm", "PersonNm")
            if not name:
                continue
            comp_str = find_text(el, "irs:ReportableCompFromOrgAmt", "ReportableCompFromOrgAmt")
            comp = None
            if comp_str:
                try:
                    comp = int(re.sub(r"[^0-9\-]", "", comp_str))
                except Exception:
                    pass
            hours_str = find_text(el, "irs:AverageHoursPerWeekRt", "AverageHoursPerWeekRt")
            hours = None
            if hours_str:
                try:
                    hours = float(hours_str)
                except Exception:
                    pass
            officers.append({
                "name": name.strip(),
                "normalized_name": norm(name),
                "name_hash": nhash(name),
                "title": find_text(el, "irs:TitleTxt", "TitleTxt"),
                "compensation": comp,
                "hours_per_week": hours,
            })

    return {
        "ein": ein,
        "tax_year": year,
        "form_type": form,
        "object_id": object_id,
        "total_revenue":           find_int(f, "irs:CYTotalRevenueAmt", "CYTotalRevenueAmt") if f is not None else None,
        "total_expenses":          find_int(f, "irs:CYTotalExpensesAmt", "CYTotalExpensesAmt") if f is not None else None,
        "total_assets":            find_int(f, "irs:TotalAssetsEOYAmt", "TotalAssetsEOYAmt") if f is not None else None,
        "net_assets":              find_int(f, "irs:NetAssetsOrFundBalancesEOYAmt", "NetAssetsOrFundBalancesEOYAmt") if f is not None else None,
        "govt_grants":             find_int(f, "irs:CYGrantsAndSimilarPaidAmt", "CYGrantsAndSimilarPaidAmt") if f is not None else None,
        "program_service_revenue": find_int(f, "irs:CYProgramServiceRevenueAmt", "CYProgramServiceRevenueAmt") if f is not None else None,
        "investment_income":       find_int(f, "irs:CYInvestmentIncomeAmt", "CYInvestmentIncomeAmt") if f is not None else None,
        "total_compensation":      find_int(f, "irs:CYSalariesCompEmpBnftPaidAmt", "CYSalariesCompEmpBnftPaidAmt") if f is not None else None,
        "mission":                 find_text(f, "irs:ActivityOrMissionDesc", "ActivityOrMissionDesc") if f is not None else None,
        "officers": officers,
    }


# ── IRS index & ZIP helpers ───────────────────────────────────

def load_index_csv(year):
    csv_url = IRS_INDEX_CSV.format(year=year)
    index_path = DOWNLOAD_DIR / f"index_{year}.csv"

    if not index_path.exists():
        log.info(f"Downloading {year} index CSV...")
        try:
            r = requests.get(csv_url, timeout=120)
            r.raise_for_status()
            index_path.write_bytes(r.content)
            log.info(f"Saved {len(r.content):,} bytes")
        except Exception as e:
            log.error(f"Index download failed: {e}")
            return []

    filings = []
    with open(index_path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            form_type = row.get("RETURN_TYPE", row.get("FormType", "")).strip()
            if form_type not in ("990", "990EZ", "990-EZ"):
                continue
            object_id = row.get("OBJECT_ID", row.get("ObjectId", "")).strip()
            ein = re.sub(r"[^0-9]", "", row.get("EIN", "").strip())
            if object_id and ein:
                filings.append({"object_id": object_id, "ein": ein, "form_type": form_type})

    log.info(f"{len(filings):,} 990/990EZ filings in {year} index")
    return filings


def check_zip_exists(url):
    try:
        r = requests.head(url, timeout=15)
        return r.status_code == 200
    except Exception:
        return False


def load_zip_index(zip_url):
    zip_name = zip_url.split("/")[-1]
    local_path = DOWNLOAD_DIR / zip_name

    if not local_path.exists():
        log.info(f"Downloading {zip_name}...")
        try:
            r = requests.get(zip_url, timeout=600, stream=True)
            r.raise_for_status()
            total = 0
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
                    total += len(chunk)
            log.info(f"Saved {total / 1024 / 1024:.1f} MB -> {local_path}")
        except Exception as e:
            log.error(f"ZIP download failed: {e}")
            if local_path.exists():
                local_path.unlink()
            return {}

    log.info(f"Indexing {zip_name}...")
    index = {}
    try:
        with zipfile.ZipFile(local_path, "r") as zf:
            for name in zf.namelist():
                if name.lower().endswith(".xml"):
                    obj_id = re.sub(r"_public\.xml$", "", name.split("/")[-1], flags=re.IGNORECASE)
                    try:
                        index[obj_id] = zf.read(name)
                    except Exception as e:
                        log.debug(f"Could not read {name}: {e}")
        log.info(f"Indexed {len(index):,} XML files")
    except zipfile.BadZipFile:
        log.error(f"Bad ZIP {zip_name} — deleting, retry next run")
        local_path.unlink()
    except Exception as e:
        log.error(f"ZIP read error: {e}")

    return index


def get_zip_urls_for_year(year):
    parts = ZIP_PARTS.get(year, ["01A"])
    urls = []
    for part in parts:
        url = IRS_ZIP_BASE.format(year=year, part=part)
        # If already cached locally, include it without a HEAD check
        local_path = DOWNLOAD_DIR / url.split("/")[-1]
        if local_path.exists():
            urls.append(url)
            log.info(f"  Cached: {url.split('/')[-1]}")
        elif check_zip_exists(url):
            urls.append(url)
            log.info(f"  Found:  {url.split('/')[-1]}")
        else:
            log.debug(f"  Not found: {url.split('/')[-1]} — skipping")
    return urls


# ── Flush batch to edge function ──────────────────────────────

def flush(batch):
    if not batch:
        return 0, 0
    r = call_edge("ingest_filings", {"filings": batch})
    if r.get("success"):
        inserted = r.get("inserted", 0)
        officers = r.get("officers_inserted", 0)
        if inserted > 0:
            # Only cache IDs that were actually confirmed inserted
            save_to_cache([f["object_id"] for f in batch])
        else:
            log.warning(f"Batch returned success but inserted=0 — full response: {r}")
        return inserted, officers
    log.error(f"Batch FAILED — error: {r.get('error')} | full response: {r}")
    return 0, 0


# ── Main ──────────────────────────────────────────────────────

def run():
    log.info("=" * 55)
    log.info("Equalyzer 990 ETL  v3  — Fast Resume Edition")
    log.info(f"Target: {MAX_FILINGS:,} filings  |  Batch size: {BATCH_SIZE}")
    log.info("=" * 55)

    check_config()

    if not ping():
        log.error(
            "\nCannot connect to edge function.\n"
            "1. Check etl-ingest is deployed in Lovable\n"
            "2. Check ETL_SECRET_TOKEN2 in .env\n"
            "3. Check SUPABASE_URL in .env\n"
        )
        return

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Build skip-set from DB + local cache
    skip_ids = load_skip_set()
    log.info(f"Will skip {len(skip_ids):,} already-processed filings\n")

    total         = 0
    officers_total = 0
    errors        = 0
    skipped       = 0
    batch         = []
    start_time    = time.time()

    def eta_str():
        elapsed = time.time() - start_time
        if total == 0:
            return "calculating..."
        rate = total / elapsed  # filings/sec
        remaining = MAX_FILINGS - total - len(skip_ids)
        if remaining <= 0 or rate <= 0:
            return "almost done"
        secs = remaining / rate
        if secs > 3600:
            return f"{secs/3600:.1f} hrs"
        return f"{secs/60:.0f} min"

    try:
        for year in [2024, 2023, 2022]:
            if total >= MAX_FILINGS:
                break

            log.info(f"\n{'─'*40}")
            log.info(f"Year {year}")
            log.info(f"{'─'*40}")

            filings_list = load_index_csv(year)
            if not filings_list:
                continue

            # Remove already-processed from needed set
            all_ids   = {f["object_id"] for f in filings_list}
            needed_ids = all_ids - skip_ids
            year_skipped = len(all_ids) - len(needed_ids)
            skipped += year_skipped
            log.info(f"  {len(all_ids):,} total  |  {year_skipped:,} skipped  |  {len(needed_ids):,} to process")

            if not needed_ids:
                log.info(f"  Year {year} fully processed — skipping")
                continue

            zip_urls = get_zip_urls_for_year(year)
            if not zip_urls:
                log.error(f"No ZIP files found for {year} — skipping")
                continue

            for zip_url in zip_urls:
                if total >= MAX_FILINGS:
                    break
                if not needed_ids:
                    break

                zip_index = load_zip_index(zip_url)
                if not zip_index:
                    continue

                matches = needed_ids & set(zip_index.keys())
                log.info(f"  {zip_url.split('/')[-1]}: {len(matches):,} new filings to ingest")

                for obj_id in matches:
                    if total >= MAX_FILINGS:
                        break

                    xml_content = zip_index[obj_id]
                    parsed = parse_xml(xml_content, obj_id)

                    if parsed is None:
                        errors += 1
                        needed_ids.discard(obj_id)
                        continue

                    batch.append(parsed)
                    needed_ids.discard(obj_id)
                    skip_ids.add(obj_id)  # prevent double-processing in same run

                    if len(batch) >= BATCH_SIZE:
                        ins, offs = flush(batch)
                        total += ins
                        officers_total += offs
                        batch = []
                        elapsed = time.time() - start_time
                        rate = total / elapsed if elapsed > 0 else 0
                        log.info(
                            f"  ✓ {total:,} filings | {officers_total:,} officers | "
                            f"{errors} errors | {rate:.1f}/sec | ETA {eta_str()}"
                        )

                del zip_index  # free memory

            log.info(f"Year {year} done.")

        # Final flush
        if batch:
            ins, offs = flush(batch)
            total += ins
            officers_total += offs

    except KeyboardInterrupt:
        log.info("\nStopped by user — flushing final batch...")
        if batch:
            ins, offs = flush(batch)
            total += ins
            officers_total += offs
        log.info("Progress saved. Run again to resume from where you left off.")

    elapsed = time.time() - start_time
    log.info("\n" + "=" * 55)
    log.info(f"SESSION COMPLETE")
    log.info(f"  New filings ingested : {total:,}")
    log.info(f"  Officers loaded      : {officers_total:,}")
    log.info(f"  Skipped (already in) : {skipped:,}")
    log.info(f"  Errors               : {errors}")
    log.info(f"  Time                 : {elapsed/60:.1f} min")
    log.info(f"  Rate                 : {total/elapsed:.1f} filings/sec" if elapsed > 0 else "")
    log.info("\nNext step: python generate_relationships.py")
    log.info("=" * 55)


if __name__ == "__main__":
    run()
