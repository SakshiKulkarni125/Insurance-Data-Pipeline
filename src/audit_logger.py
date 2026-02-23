import os
from datetime import datetime
import csv


def write_log(log_file, message):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    with open(log_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([datetime.now(), message])


def log_ingestion(file_name):
    log_file = os.path.join("..", "logs", "ingestion_log.csv")
    write_log(log_file, f"Ingested file: {file_name}")


def log_preprocess(file_name):
    log_file = os.path.join("..", "logs", "preprocess_log.csv")
    write_log(log_file, f"Preprocessed file: {file_name}")


def log_retention(file_name):
    log_file = os.path.join("..", "logs", "retention_log.csv")
    write_log(log_file, f"Archived file: {file_name}")