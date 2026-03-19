"""
extract.py — Extract Phase
Reads raw CSV data and returns a DataFrame for transformation.
"""

import pandas as pd
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [EXTRACT] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

RAW_DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "telco_churn_raw.csv")


def extract_data(filepath: str = RAW_DATA_PATH) -> pd.DataFrame:
    """
    Reads raw CSV file and returns a DataFrame.
    Logs row/column count and any immediate issues.
    """
    logging.info(f"Reading raw data from: {filepath}")

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Raw data file not found: {filepath}")

    df = pd.read_csv(filepath)
    logging.info(f"Extracted {len(df)} rows and {len(df.columns)} columns.")
    logging.info(f"Columns: {list(df.columns)}")

    # Basic validation
    if df.empty:
        raise ValueError("Extracted DataFrame is empty.")

    missing_summary = df.isnull().sum()
    missing_cols = missing_summary[missing_summary > 0]
    if not missing_cols.empty:
        logging.warning(f"Missing values detected:\n{missing_cols}")

    return df


if __name__ == "__main__":
    df = extract_data()
    print(df.head())
