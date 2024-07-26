import os
import sys
sys.path.append('/opt/cleo')
import psycopg2
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
from dbconnection import DBConnection
from PIL import Image
import cv2
import hashlib
from psycopg2.extras import execute_values

# Load environment variables from .env file
load_dotenv()

class ValidationScript:
    def __init__(self):
        self.db_conn_instance = DBConnection.get_instance()
        self.image_directory = Path('/mnt/MOM/Images')
        self.error_directory = Path('/mnt/MOM/Errors')
        self.error_table = 'error_files'
        self.create_error_table()
        self.image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.tiff', '.ico', '.heic'}

    def create_error_table(self):
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.error_table} (
            filename TEXT PRIMARY KEY,
            tensor_pil BYTEA,
            hash_pil TEXT,
            tensor_cv2 BYTEA,
            hash_cv2 TEXT
        )
        """
        connection = self.db_conn_instance.get_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()
        self.db_conn_instance.return_connection(connection)

    def insert_into_error_table(self, error_files):
        query = f"""
        INSERT INTO {self.error_table} (filename, tensor_pil, hash_pil, tensor_cv2, hash_cv2)
        VALUES %s
        ON CONFLICT (filename) DO UPDATE
        SET tensor_pil = EXCLUDED.tensor_pil,
            hash_pil = EXCLUDED.hash_pil,
            tensor_cv2 = EXCLUDED.tensor_cv2,
            hash_cv2 = EXCLUDED.hash_cv2
        """
        values = [
            (str(file), pil_tensor.tobytes() if pil_tensor is not None else None, pil_hash, cv2_tensor.tobytes() if cv2_tensor is not None else None, cv2_hash)
            for file, pil_tensor, pil_hash, cv2_tensor, cv2_hash in error_files
        ]
        connection = self.db_conn_instance.get_connection()
        cursor = connection.cursor()
        execute_values(cursor, query, values)
        connection.commit()
        cursor.close()
        self.db_conn_instance.return_connection(connection)

    def generate_tensors(self, file):
        try:
            pil_image = Image.open(file).convert('RGB')
            pil_tensor = np.array(pil_image.resize((50, 50), Image.BICUBIC))
            cv2_image = cv2.imread(str(file))
            cv2_tensor = cv2.resize(cv2.cvtColor(cv2_image, cv2.COLOR_BGR2RGB), (50, 50))

            pil_hash = hashlib.md5(pil_tensor.tobytes()).hexdigest()
            cv2_hash = hashlib.md5(cv2_tensor.tobytes()).hexdigest()

            return pil_tensor, pil_hash, cv2_tensor, cv2_hash
        except Exception as e:
            print(f"Failed to generate tensors for {file}: {e}")
            return None, None, None, None

    def get_error_files(self):
        error_files = [file for file in self.error_directory.glob('**/*') if file.suffix.lower() in self.image_extensions]
        return error_files

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

    def validate_and_rename(self, db_files, error_files):
        updated_db_files = []

        for idx, db_file in enumerate(db_files):
            db_id, db_name, db_tensor_pil, db_hash_pil, db_tensor_cv2, db_hash_cv2 = db_file
            matched = False
            print(f"Processing {idx + 1}/{len(db_files)}: {db_name}")

            for error_file in error_files:
                filename, tensor_pil, hash_pil, tensor_cv2, hash_cv2 = error_file
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
        error_files = self.get_error_files()
        error_files_with_tensors = []

        print(f"Found {len(error_files)} image files in the error directory.")

        for idx, file in enumerate(error_files):
            print(f"Processing {idx + 1}/{len(error_files)}: {file}")
            pil_tensor, pil_hash, cv2_tensor, cv2_hash = self.generate_tensors(file)
            error_files_with_tensors.append((str(file), pil_tensor, pil_hash, cv2_tensor, cv2_hash))

        self.insert_into_error_table(error_files_with_tensors)

        db_files_without_directory = self.get_db_files_without_directory()

        print(f"Found {len(db_files_without_directory)} files in the database without corresponding files in the directory.")

        updated_db_files = self.validate_and_rename(db_files_without_directory, error_files_with_tensors)

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
