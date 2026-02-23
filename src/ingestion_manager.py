import os
import json
from utils import read_csv_file
from audit_logger import log_ingestion

# Function to validate one file
def validate_file(csv_path, json_path):
    df = read_csv_file(csv_path)
    if df is None:
        return False

    # Load expected schema
    with open(json_path, 'r') as f:
        control_data = json.load(f)

    expected_columns = control_data["expected_columns"]

    # Check if all expected columns exist
    if set(expected_columns).issubset(set(df.columns)):
        print(f"Validation PASSED: {csv_path}")
        log_ingestion(os.path.basename(csv_path))
        return True
    else:
        print(f"Validation FAILED: {csv_path}")
        return False


# Function to validate all files in ingestion folder
def validate_all_files(ingestion_folder, control_folder):
    valid_files = []

    for file in os.listdir(ingestion_folder):
        if file.endswith(".csv"):

            csv_path = os.path.join(ingestion_folder, file)

            # Get matching JSON control file name
            base_name = file.split("_")[0]
            json_name = f"{base_name}_yyyymmdd.json"
            json_path = os.path.join(control_folder, json_name)

            if os.path.exists(json_path):
                if validate_file(csv_path, json_path):
                    valid_files.append(csv_path)
            else:
                print(f"No control file found for {file}")

    return valid_files