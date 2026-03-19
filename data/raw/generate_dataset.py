import pandas as pd
import numpy as np
import os

np.random.seed(42)
n = 7043

customer_ids = [f"{''.join(np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 4))}-{''.join(np.random.choice(list('0123456789'), 4))}" for _ in range(n)]

df = pd.DataFrame({
    "customerID": customer_ids,
    "gender": np.random.choice(["Male", "Female"], n),
    "SeniorCitizen": np.random.choice([0, 1], n, p=[0.84, 0.16]),
    "Partner": np.random.choice(["Yes", "No"], n),
    "Dependents": np.random.choice(["Yes", "No"], n, p=[0.3, 0.7]),
    "tenure": np.random.randint(0, 72, n),
    "PhoneService": np.random.choice(["Yes", "No"], n, p=[0.9, 0.1]),
    "MultipleLines": np.random.choice(["Yes", "No", "No phone service"], n, p=[0.42, 0.48, 0.10]),
    "InternetService": np.random.choice(["DSL", "Fiber optic", "No"], n, p=[0.34, 0.44, 0.22]),
    "OnlineSecurity": np.random.choice(["Yes", "No", "No internet service"], n, p=[0.29, 0.49, 0.22]),
    "OnlineBackup": np.random.choice(["Yes", "No", "No internet service"], n, p=[0.34, 0.44, 0.22]),
    "DeviceProtection": np.random.choice(["Yes", "No", "No internet service"], n, p=[0.34, 0.44, 0.22]),
    "TechSupport": np.random.choice(["Yes", "No", "No internet service"], n, p=[0.29, 0.49, 0.22]),
    "StreamingTV": np.random.choice(["Yes", "No", "No internet service"], n, p=[0.38, 0.40, 0.22]),
    "StreamingMovies": np.random.choice(["Yes", "No", "No internet service"], n, p=[0.39, 0.39, 0.22]),
    "Contract": np.random.choice(["Month-to-month", "One year", "Two year"], n, p=[0.55, 0.21, 0.24]),
    "PaperlessBilling": np.random.choice(["Yes", "No"], n, p=[0.59, 0.41]),
    "PaymentMethod": np.random.choice([
        "Electronic check", "Mailed check",
        "Bank transfer (automatic)", "Credit card (automatic)"
    ], n, p=[0.34, 0.23, 0.22, 0.21]),
    "MonthlyCharges": np.round(np.random.uniform(18.0, 118.0, n), 2),
    "TotalCharges": [""] * n,  # some blanks to simulate dirty data
    "Churn": np.random.choice(["Yes", "No"], n, p=[0.265, 0.735]),
})

# Populate TotalCharges with some blanks (new customers)
for i in range(n):
    if df.loc[i, "tenure"] == 0:
        df.loc[i, "TotalCharges"] = " "
    else:
        df.loc[i, "TotalCharges"] = str(round(df.loc[i, "tenure"] * df.loc[i, "MonthlyCharges"] * np.random.uniform(0.9, 1.1), 2))

os.makedirs("data/raw", exist_ok=True)
df.to_csv("data/raw/telco_churn_raw.csv", index=False)
print(f"Dataset generated: {len(df)} rows, {len(df.columns)} columns")
print(df.head(3))
