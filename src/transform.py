"""
transform.py — Transform Phase
Cleans and engineers features from raw Telco churn data.
"""

import pandas as pd
import numpy as np
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TRANSFORM] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

PROCESSED_DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "telco_churn_cleaned.csv")


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies all transformation steps:
    1. Fix TotalCharges (blank → NaN → median imputation)
    2. Convert binary columns to 0/1
    3. Drop irrelevant columns
    4. Feature engineering: tenure_group, avg_monthly_spend
    5. Standardize Churn column to binary
    """
    logging.info("Starting transformation...")
    df = df.copy()

    # Step 1: Fix TotalCharges
    logging.info("Step 1: Fixing TotalCharges column...")
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"].str.strip(), errors="coerce")
    median_total = df["TotalCharges"].median()
    nulls_before = df["TotalCharges"].isnull().sum()
    df["TotalCharges"] = df["TotalCharges"].fillna(median_total)
    logging.info(f"  Imputed {nulls_before} null TotalCharges values with median ({median_total:.2f})")

    # Step 2: Convert binary Yes/No columns to 1/0
    logging.info("Step 2: Encoding binary columns...")
    binary_cols = ["Partner", "Dependents", "PhoneService", "PaperlessBilling", "Churn"]
    for col in binary_cols:
        if col in df.columns:
            df[col] = df[col].map({"Yes": 1, "No": 0})

    # Step 3: Drop customerID (not useful for analysis)
    logging.info("Step 3: Dropping customerID column...")
    df.drop(columns=["customerID"], inplace=True, errors="ignore")

    # Step 4: Feature Engineering
    logging.info("Step 4: Engineering new features...")

    # tenure_group: bucket customers into segments
    def bucket_tenure(t):
        if t <= 12:
            return "0-1 year"
        elif t <= 24:
            return "1-2 years"
        elif t <= 48:
            return "2-4 years"
        else:
            return "4+ years"

    df["tenure_group"] = df["tenure"].apply(bucket_tenure)

    # avg_monthly_spend per tenure month
    df["avg_monthly_spend"] = np.where(
        df["tenure"] > 0,
        (df["TotalCharges"] / df["tenure"]).round(2),
        df["MonthlyCharges"]
    )

    # Step 5: Standardize gender
    logging.info("Step 5: Standardizing gender column...")
    df["gender"] = df["gender"].str.strip().str.title()

    logging.info(f"Transformation complete. Final shape: {df.shape}")
    logging.info(f"Churn rate: {df['Churn'].mean()*100:.2f}%")

    # Save processed file
    os.makedirs(os.path.dirname(PROCESSED_DATA_PATH), exist_ok=True)
    df.to_csv(PROCESSED_DATA_PATH, index=False)
    logging.info(f"Processed data saved to: {PROCESSED_DATA_PATH}")

    return df


if __name__ == "__main__":
    from extract import extract_data
    raw_df = extract_data()
    clean_df = transform_data(raw_df)
    print(clean_df.head())
    print(f"\nShape: {clean_df.shape}")
    print(f"\nNew columns: tenure_group, avg_monthly_spend")
