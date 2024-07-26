import os
import psycopg2
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
import sys
sys.path.append('/opt/cleo')
from dbconnection import DBConnection

# Load environment variables from .env file
load_dotenv()

class ValidationScript:
    def __init__(self):
        self.db_conn_instance = DBConnection.get_instance()
        self.image_directory = Path('/mnt/MOM/Images')
        self.unmatched_table = 'unmatched_files'

    def get_unmatched_files(self):
        query = f"""
        SELECT filename, tensor_pil, hash_pil, tensor_cv2, hash_cv2
        FROM {self.unmatched_table}
        """
        connection = self.db_conn_instance.get_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        unmatched_files = cursor.fetchall()
        cursor.close()
        self.db_conn_instance.return_connection(connection)
        return unmatched_files

    def get_db_files_without_directory(self):
        query = """
        SELECT mo.media_object_id, mo.new_name, it.tensor_pil, it.hash_pil, it.tensor_cv2, it.hash_cv2
        FROM tbl_media_objects mo
        JOIN tbl_image_tensors it ON mo.image_tensor_id = it.id
        WHERE mo.media_type = 'image' AND mo.new_name IS NOT NULL
        """
        connection = self.db_conn_instance.get_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        db_files = cursor.fetchall()
        cursor.close()
        self.db_conn_instance.return_connection(connection)

        db_files_without_directory = []
        for db_file in db_files:
            db_id, db_name, db_tensor_pil, db_hash_pil, db_tensor_cv2, db_hash_cv2 = db_file
            file_path = self.image_directory / db_name
            if not file_path.exists():
                db_files_without_directory.append(db_file)
        
        return db_files_without_directory

    def validate_and_rename(self, db_files, unmatched_files):
        updated_db_files = []

        for idx, db_file in enumerate(db_files):
            db_id, db_name, db_tensor_pil, db_hash_pil, db_tensor_cv2, db_hash_cv2 = db_file
            matched = False
            print(f"Processing {idx + 1}/{len(db_files)}: {db_name}")

            for unmatched_file in unmatched_files:
                filename, tensor_pil, hash_pil, tensor_cv2, hash_cv2 = unmatched_file
                if self.validate_tensors(db_tensor_pil, tensor_pil) and self.validate_tensors(db_tensor_cv2, tensor_cv2):
                    old_file_path = Path(filename)
                    new_file_path = self.image_directory / db_name
                    if old_file_path != new_file_path:
                        try:
                            os.rename(old_file_path, new_file_path)
                            print(f"Renamed {old_file_path} to {new_file_path}")
                        except FileNotFoundError:
                            print(f"File {old_file_path} not found for renaming.")
                    matched = True
                    break

            if not matched:
                updated_db_files.append(db_file)

        return updated_db_files

    def validate_tensors(self, tensor1, tensor2):
        if tensor1 is None or tensor2 is None:
            return False
        return np.array_equal(np.frombuffer(tensor1, dtype=np.uint8), np.frombuffer(tensor2, dtype=np.uint8))

    def run(self):
        unmatched_files = self.get_unmatched_files()
        db_files_without_directory = self.get_db_files_without_directory()

        print(f"Found {len(db_files_without_directory)} files in the database without corresponding files in the directory.")
        print(f"Found {len(unmatched_files)} unmatched files in the directory.")

        updated_db_files = self.validate_and_rename(db_files_without_directory, unmatched_files)

        print("Updated file matching complete.")
        print(f"Remaining unmatched files in the database: {len(updated_db_files)}")

        # Save the updated lists to a report file
        with open('updated_report.txt', 'w') as report_file:
            report_file.write(f"Remaining unmatched files in the database: {len(updated_db_files)}\n")
            for db_file in updated_db_files:
                report_file.write(f"{db_file}\n")

if __name__ == "__main__":
    validator = ValidationScript()
    validator.run()
    print("Validation process completed.")
