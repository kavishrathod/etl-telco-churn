"""
load.py — Load Phase
Loads transformed data into MySQL database.
Falls back to CSV export if MySQL is unavailable.
"""

import pandas as pd
import logging
import os
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [LOAD] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "outputs", "logs", "load_log.json")
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "..", "outputs", "telco_churn_final.csv")

# MySQL config — update with credentials to load into MySQl else CSV 
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "your_password",
    "database": "telco_etl"
}

TABLE_NAME = "customer_churn"


def load_to_mysql(df: pd.DataFrame, config: dict = MYSQL_CONFIG, table: str = TABLE_NAME) -> dict:
    """
    Loads DataFrame into MySQL table.
    Creates table if not exists.
    Returns a log dict with row counts.
    """
    try:
        import mysql.connector
        from mysql.connector import Error

        logging.info(f"Connecting to MySQL database: {config['database']}")
        conn = mysql.connector.connect(**config)

        if conn.is_connected():
            cursor = conn.cursor()

            # Create DB if not exists
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['database']}")
            cursor.execute(f"USE {config['database']}")

            # Drop and recreate table
            cursor.execute(f"DROP TABLE IF EXISTS {table}")

            create_sql = f"""
            CREATE TABLE {table} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                gender VARCHAR(10),
                SeniorCitizen INT,
                Partner INT,
                Dependents INT,
                tenure INT,
                PhoneService INT,
                MultipleLines VARCHAR(30),
                InternetService VARCHAR(20),
                OnlineSecurity VARCHAR(30),
                OnlineBackup VARCHAR(30),
                DeviceProtection VARCHAR(30),
                TechSupport VARCHAR(30),
                StreamingTV VARCHAR(30),
                StreamingMovies VARCHAR(30),
                Contract VARCHAR(20),
                PaperlessBilling INT,
                PaymentMethod VARCHAR(40),
                MonthlyCharges FLOAT,
                TotalCharges FLOAT,
                Churn INT,
                tenure_group VARCHAR(20),
                avg_monthly_spend FLOAT
            )
            """
            cursor.execute(create_sql)
            logging.info(f"Table `{table}` created.")

            # Insert rows
            cols = ", ".join(df.columns)
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

            rows = [tuple(row) for row in df.itertuples(index=False)]
            cursor.executemany(insert_sql, rows)
            conn.commit()

            # Row count validation
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            db_count = cursor.fetchone()[0]

            logging.info(f"Loaded {db_count} rows into `{table}`.")
            cursor.close()
            conn.close()

            return {"status": "success", "rows_inserted": len(df), "rows_in_db": db_count, "method": "mysql"}

    except Exception as e:
        logging.warning(f"MySQL load failed: {e}")
        logging.info("Falling back to CSV export...")
        return load_to_csv(df)


def load_to_csv(df: pd.DataFrame) -> dict:
    """
    Fallback: saves transformed data to CSV output.
    """
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    logging.info(f"Data saved to CSV: {OUTPUT_CSV} ({len(df)} rows)")
    return {"status": "success", "rows_inserted": len(df), "method": "csv", "path": OUTPUT_CSV}


def save_log(log_data: dict):
    """Saves run log as JSON for audit trail."""
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    log_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_PATH, "w") as f:
        json.dump(log_data, f, indent=4)
    logging.info(f"Run log saved: {LOG_PATH}")


def load_data(df: pd.DataFrame) -> dict:
    """Main load function called by pipeline."""
    result = load_to_mysql(df)
    save_log(result)
    return result


if __name__ == "__main__":
    import sys
    sys.path.append(os.path.dirname(__file__))
    from extract import extract_data
    from transform import transform_data

    raw = extract_data()
    cleaned = transform_data(raw)
    result = load_data(cleaned)
    print(f"\nLoad Result: {result}")
