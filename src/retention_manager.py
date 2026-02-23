import os
import shutil
from audit_logger import log_retention


def archive_files(valid_files, retention_folder):

    for file_path in valid_files:

        file_name = os.path.basename(file_path)
        destination = os.path.join(retention_folder, file_name)

        shutil.move(file_path, destination)

        print(f"Archived: {file_name}")

        log_retention(file_name)