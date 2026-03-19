"""
pipeline.py — Main ETL Pipeline Runner
Orchestrates Extract → Transform → Load for Telco Customer Churn data.

Usage:
    python pipeline.py
"""

import logging
import time
import sys
import os

sys.path.append(os.path.dirname(__file__))

from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PIPELINE] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def run_pipeline():
    logging.info("   ETL PIPELINE STARTED — Telco Customer Churn")

    start = time.time()

    # ── EXTRACT ──────────────────────────────────────────────
    logging.info("\n[1/3] EXTRACT PHASE")
    raw_df = extract_data()
    logging.info(f"  ✔ Extracted {len(raw_df)} records")

    # ── TRANSFORM ────────────────────────────────────────────
    logging.info("\n[2/3] TRANSFORM PHASE")
    clean_df = transform_data(raw_df)
    logging.info(f"  ✔ Transformed data — shape: {clean_df.shape}")
    logging.info(f"  ✔ Churn rate: {clean_df['Churn'].mean()*100:.2f}%")

    # ── LOAD ─────────────────────────────────────────────────
    logging.info("\n[3/3] LOAD PHASE")
    result = load_data(clean_df)
    logging.info(f"  ✔ Load method: {result.get('method', 'unknown')}")
    logging.info(f"  ✔ Rows inserted: {result.get('rows_inserted', 0)}")

    # ── ROW COUNT VALIDATION ─────────────────────────────────
    input_rows = len(raw_df)
    output_rows = result.get("rows_inserted", 0)

    if input_rows == output_rows:
        logging.info(f"\n  ✅ Validation PASSED — {input_rows} rows in == {output_rows} rows out")
    else:
        logging.warning(f"\n  ⚠️  Validation MISMATCH — Input: {input_rows}, Output: {output_rows}")

    elapsed = round(time.time() - start, 2)
    logging.info(f"   PIPELINE COMPLETED in {elapsed}s")

    return result


if __name__ == "__main__":
    run_pipeline()
