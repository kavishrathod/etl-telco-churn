# 📦 ETL Data Pipeline — Telco Customer Churn

An end-to-end **ETL (Extract, Transform, Load) pipeline** built in Python to process **7,043 telecom customer records** for churn analysis. Designed a modular ETL pipeline with logging, validation, and fallback handling. The pipeline ingests raw CSV data, applies data cleaning and feature engineering, and loads structured output into a **MySQL database** (with CSV fallback).
- I built this project to get hands-on with ETL pipelines after studying data engineering concepts. The Telco Churn dataset from Kaggle was a good choice because it had real dirty data issues — the TotalCharges column had blank strings that needed to be handled before loading.


---

## 🗂️ Project Structure

```
etl-telco-churn/
├── data/
│   ├── raw/
│   │   ├── telco_churn_raw.csv        # Raw input dataset (7,043 records)
│   └── processed/
│       └── telco_churn_cleaned.csv    # Output of Transform phase
├── src/
│   ├── extract.py                     # Extract phase — reads raw CSV
│   ├── transform.py                   # Transform phase — cleans & engineers features
│   └── load.py                        # Load phase — MySQL or CSV output
├── outputs/
│   ├── telco_churn_final.csv          # Final loaded output
│   └── logs/
│       └── load_log.json              # Pipeline run audit log
├── pipeline.py                        # Main pipeline orchestrator
├── requirements.txt
└── README.md
```

---

## ⚙️ Pipeline Overview

```
[Raw CSV] → EXTRACT → TRANSFORM → LOAD → [MySQL / CSV]
```

### 1. Extract (`src/extract.py`)
- Reads raw CSV from `data/raw/`
- Validates file existence and checks for missing values
- Logs row/column counts

### 2. Transform (`src/transform.py`)
- **Fixes `TotalCharges`** — converts blank strings to `NaN`, imputes with median
- **Encodes binary columns** — maps `Yes/No` to `1/0` (Partner, Dependents, Churn, etc.)
- **Drops `customerID`** — not needed for analysis
- **Feature Engineering:**
  - `tenure_group` — buckets customers into `0-1 year`, `1-2 years`, `2-4 years`, `4+ years`
  - `avg_monthly_spend` — calculates average monthly spend per customer
- **Standardizes** gender column formatting

### 3. Load (`src/load.py`)
- Loads cleaned data into **MySQL** (`telco_etl.customer_churn` table)
- **Falls back to CSV** automatically if MySQL is unavailable
- Saves a **JSON audit log** with timestamp, row counts, and load method
- **Row-count validation** ensures no data loss between Extract and Load

---

## 📊 Key Insights from Data

| Metric | Value |
|---|---|
| Total Records | 7,043 |
| Churn Rate | ~26% |
| Blank TotalCharges fixed | ~100 records |
| New features engineered | 2 (`tenure_group`, `avg_monthly_spend`) |

---

## 🚀 How to Run

### 1. Clone the repo
```bash
git clone https://github.com/kavishrathod/etl-telco-churn.git
cd etl-telco-churn
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. (Optional) Configure MySQL
Edit `src/load.py` and update `MYSQL_CONFIG` with your credentials:
```python
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "your_password",
    "database": "telco_etl"
}
```
> If MySQL is not set up, the pipeline automatically saves output to `outputs/telco_churn_final.csv`

### 4. Run the full pipeline
```bash
python pipeline.py
```

### Expected Output
```
[PIPELINE] ETL PIPELINE STARTED — Telco Customer Churn
[1/3] EXTRACT PHASE — ✔ Extracted 7043 records
[2/3] TRANSFORM PHASE — ✔ Transformed data — shape: (7043, 22)
[3/3] LOAD PHASE — ✔ Rows inserted: 7043
✅ Validation PASSED — 7043 rows in == 7043 rows out
PIPELINE COMPLETED in 0.25s
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.10+ | Core language |
| Pandas | Data manipulation & transformation |
| NumPy | Numerical operations |
| MySQL | Structured data storage |
| mysql-connector-python | MySQL integration |
| JSON | Audit logging |

---

## 📈 Power BI Dashboard

A Power BI dashboard was built on top of the cleaned output to visualize:
- **Churn rate** by contract type and tenure group
- **Revenue breakdown** by payment method and internet service
- **Monthly charges** distribution for churned vs. retained customers
- **KPIs:** Total customers, churn count, average monthly revenue

---

## 🔮 Future Work

Cloud Migration: Move the pipeline to AWS S3 for storage and Amazon Redshift for scalable data warehousing

Workflow Orchestration: Integrate Apache Airflow to automate scheduling, monitoring, and pipeline retries

---

## 👤 Author

**Kavish Rathod**  
BE Graduate — Electronics & Telecommunication, VCET Mumbai  
[LinkedIn](https://linkedin.com/in/kavishrathod) • [GitHub](https://github.com/kavishrathod)
