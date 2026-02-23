import os
import pandas as pd
from utils import read_csv_file, save_parquet, get_today_date
from audit_logger import log_preprocess


# Function to extract date from filename
def extract_file_date(file_name):
    date_part = file_name.split("_")[1].split(".")[0]
    return pd.to_datetime(date_part, format='%Y%m%d')


# Function to preprocess one file
def preprocess_file(csv_path, output_folder):

    df = read_csv_file(csv_path)
    if df is None:
        return

    # Remove duplicates
    df = df.drop_duplicates()

    # Add ingestion date
    df["ingestion_date"] = get_today_date()

    # Add file date
    file_name = os.path.basename(csv_path)
    df["file_date"] = extract_file_date(file_name)

    # Save as parquet
    base_name = file_name.replace(".csv", ".parquet")
    output_path = os.path.join(output_folder, base_name)

    save_parquet(df, output_path)

    log_preprocess(os.path.basename(csv_path))


# Function to preprocess all validated files
def preprocess_all_files(valid_files, output_folder):

    for file_path in valid_files:
        preprocess_file(file_path, output_folder)