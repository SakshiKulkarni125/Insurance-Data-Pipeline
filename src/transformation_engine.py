import os
import pandas as pd


# Function to load parquet files
def load_data(folder_path, file_keyword):
    for file in os.listdir(folder_path):
        if file_keyword in file and file.endswith(".parquet"):
            return pd.read_parquet(os.path.join(folder_path, file))
    return None


def build_curated_and_semantic(preprocessed_folder, curated_folder, semantic_folder):

    # Load datasets
    claims = load_data(preprocessed_folder, "claims")
    policies = load_data(preprocessed_folder, "policies")
    customers = load_data(preprocessed_folder, "customers")
    payments = load_data(preprocessed_folder, "payments")

    # Merge datasets safely
    merged = claims.merge(policies, on="Policy_ID", suffixes=("", "_pol"))
    merged = merged.merge(customers, on="Customer_ID", suffixes=("", "_cust"))
    merged = merged.merge(payments, on="Policy_ID", suffixes=("", "_pay"))

    # Save curated dataset
    curated_path = os.path.join(curated_folder, "claims_enriched.parquet")
    merged.to_parquet(curated_path, index=False)
    print("Curated dataset saved")

    # REPORT 1 — Total claim by Policy Type
    report1 = merged.groupby("Policy_Type")["Claim_Amount"].sum().reset_index()
    report1.to_csv(os.path.join(semantic_folder, "claim_by_policy_type.csv"), index=False)

    # REPORT 2 — Average claim by City
    report2 = merged.groupby("City")["Claim_Amount"].mean().reset_index()
    report2.to_csv(os.path.join(semantic_folder, "avg_claim_by_city.csv"), index=False)

    print("Reports generated")