import os

from ingestion_manager import validate_all_files
from preprocessing_engine import preprocess_all_files
from transformation_engine import build_curated_and_semantic
from retention_manager import archive_files


def run_pipeline():

    base_path = os.path.dirname(os.path.dirname(__file__))

    ingestion_folder = os.path.join(base_path, "data", "ingestion")
    control_folder = os.path.join(base_path, "config", "control_files")
    preprocessed_folder = os.path.join(base_path, "data", "preprocessed")
    curated_folder = os.path.join(base_path, "data", "curated")
    semantic_folder = os.path.join(base_path, "data", "semantic")
    retention_folder = os.path.join(base_path, "data", "retention")

    print("STEP 1 — Validating files...")
    valid_files = validate_all_files(ingestion_folder, control_folder)

    print("STEP 2 — Preprocessing files...")
    preprocess_all_files(valid_files, preprocessed_folder)

    print("STEP 3 — Transforming data...")
    build_curated_and_semantic(preprocessed_folder, curated_folder, semantic_folder)

    print("STEP 4 — Archiving files...")
    archive_files(valid_files, retention_folder)

    print("Pipeline Completed Successfully!")


if __name__ == "__main__":
    run_pipeline()