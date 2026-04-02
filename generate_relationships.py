"""
Equalyzer — Relationship Edge Generator
========================================
Calls the etl-ingest edge function to generate relationship
edges from loaded officer and award data.

Run after etl_990_v3.py completes:
  python generate_relationships.py
"""

import os
import time
import logging
import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler("relationships.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
ETL_TOKEN    = os.getenv("ETL_SECRET_TOKEN2")   # fixed: was ETL_SECRET_TOKEN
INGEST_URL   = f"{SUPABASE_URL}/functions/v1/etl-ingest"
BATCH_SIZE   = 200


def call_ingest(action, data):
    try:
        res = requests.post(
            INGEST_URL,
            headers={
                "Content-Type": "application/json",
                "x-etl-token": ETL_TOKEN,
            },
            json={"action": action, "data": data},
            timeout=90,
        )
        res.raise_for_status()
        return res.json()
    except Exception as e:
        log.error(f"Call failed: {e}")
        return {"success": False, "error": str(e)}


def run():
    log.info("=" * 60)
    log.info("Equalyzer Relationship Generator")
    log.info("=" * 60)

    if not SUPABASE_URL or not ETL_TOKEN:
        log.error("Missing SUPABASE_URL or ETL_SECRET_TOKEN2 in .env")
        return

    # Step 1: Link awards to orgs by EIN
    log.info("\nStep 1: Linking awards to organizations...")
    total_linked = 0
    while True:
        result = call_ingest("link_awards", {"limit": 500})
        if not result.get("success"):
            log.error(f"Award linking failed: {result.get('error')}")
            break
        linked = result.get("linked", 0)
        total_linked += linked
        log.info(f"  Linked {linked} awards this batch ({total_linked} total)")
        if linked < 500:
            break
        time.sleep(1)

    log.info(f"Step 1 complete: {total_linked} awards linked")

    # Step 2: Generate officer edges in batches
    log.info("\nStep 2: Generating officer relationship edges...")
    total_edges = 0
    offset = 0
    while True:
        result = call_ingest("generate_edges", {
            "batch_size": BATCH_SIZE,
            "offset": offset,
        })
        if not result.get("success"):
            log.error(f"Edge generation failed: {result.get('error')}")
            break

        inserted  = result.get("inserted", 0)
        has_more  = result.get("has_more", False)
        total_edges += inserted
        offset    = result.get("next_offset", offset + BATCH_SIZE)

        log.info(f"  Batch offset {offset - BATCH_SIZE}: {inserted} edges ({total_edges} total)")

        if not has_more:
            break
        time.sleep(1)

    log.info(f"Step 2 complete: {total_edges} relationship edges created")
    log.info("\nFinal step in Supabase SQL editor:")
    log.info("  SELECT * FROM eq_compute_org_scores();")
    log.info("  SELECT * FROM eq_compute_person_scores();")


if __name__ == "__main__":
    run()
