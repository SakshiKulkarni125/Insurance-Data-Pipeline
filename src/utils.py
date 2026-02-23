import pandas as pd
from datetime import datetime
import os

# Function to read CSV file
def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"File loaded successfully: {file_path}")
        return df
    except Exception as e:
        print(f"Error reading file: {e}")
        return None


# Function to save dataframe as parquet
def save_parquet(df, output_path):
    try:
        df.to_parquet(output_path, index=False)
        print(f"File saved successfully: {output_path}")
    except Exception as e:
        print(f"Error saving file: {e}")


# Function to get today's date
def get_today_date():
    return datetime.today().strftime('%Y-%m-%d')