import os
import hashlib
import numpy as np
import cv2
from PIL import Image, UnidentifiedImageError
from pathlib import Path
from dotenv import load_dotenv
from psycopg2.extras import execute_values
import sys
sys.path.append('/opt/cleo')
from dbconnection import DBConnection

# Load environment variables from .env file
load_dotenv()

# Define the directories
IMAGE_DIRECTORY = '/mnt/MOM/Images'
ERROR_DIRECTORY = '/mnt/MOM/Errors'

class ValidationScript:
    def __init__(self):
        self.image_directory = Path(IMAGE_DIRECTORY)
        self.error_directory = Path(ERROR_DIRECTORY)
        self.db_conn_instance = DBConnection.get_instance()
    
    def create_table(self, conn):
        query = """
        CREATE TABLE IF NOT EXISTS unmatched_files (
            filename TEXT,
            tensor_pil BYTEA,
            hash_pil TEXT,
            tensor_cv2 BYTEA,
            hash_cv2 TEXT
        );
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()

    def insert_into_table(self, conn, data):
        query = """
        INSERT INTO unmatched_files (filename, tensor_pil, hash_pil, tensor_cv2, hash_cv2)
        VALUES %s;
        """
        with conn.cursor() as cursor:
            execute_values(cursor, query, data)
            conn.commit()

    def generate_tensors(self, file):
        try:
            tensor_pil = self.generate_tensor_pil(file)
            tensor_cv2 = self.generate_tensor_cv2(file)

            if tensor_pil is not None and tensor_cv2 is not None:
                hash_pil = self.compute_tensor_hash(tensor_pil)
                hash_cv2 = self.compute_tensor_hash(tensor_cv2)

                return (tensor_pil, hash_pil, tensor_cv2, hash_cv2)
            else:
                print(f"Failed to generate tensors for file: {file}")
                return (None, None, None, None)
        except UnidentifiedImageError:
            print(f"UnidentifiedImageError: file {file} could not be identified as image.")
            return (None, None, None, None)
        except Exception as e:
            print(f"Error generating tensor for file {file}: {e}")
            return (None, None, None, None)

    def generate_tensor_pil(self, file):
        try:
            img = Image.open(file).convert('RGB')
            img = img.resize((50, 50), Image.BICUBIC)
            tensor = np.array(img)
            return tensor
        except UnidentifiedImageError:
            print(f"Error: {file} is not a valid image.")
            return None
        except OSError as e:
            print(f"Error: {file} could not be processed with PIL. OSError: {e}")
            return None

    def generate_tensor_cv2(self, file):
        try:
            img = cv2.imread(str(file))
            if img is None:
                raise ValueError(f"cv2 could not open the image: {file}")
            img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            img = cv2.resize(img, (50, 50), interpolation=cv2.INTER_CUBIC)
            tensor = np.array(img)
            return tensor
        except Exception as e:
            print(f"Error: {file} could not be processed with cv2. Error: {e}")
            return None

    def compute_tensor_hash(self, tensor):
        return hashlib.md5(tensor.tobytes()).hexdigest()

    def get_db_images(self):
        query = "SELECT new_name, new_path FROM tbl_media_objects WHERE media_type = 'image'"
        connection = self.db_conn_instance.get_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        images = cursor.fetchall()
        cursor.close()
        self.db_conn_instance.return_connection(connection)
        return images

    def run(self):
        conn = self.db_conn_instance.get_connection()
        
        self.create_table(conn)

        db_images = self.get_db_images()
        db_images_set = {Path(path) / name for name, path in db_images if path and name}

        directory_files = list(self.image_directory.glob('**/*'))
        unmatched_files = [file for file in directory_files if file not in db_images_set]

        total_unmatched_files = len(unmatched_files)
        processed_data = []
        
        for idx, file in enumerate(unmatched_files):
            print(f"Processing {idx + 1}/{total_unmatched_files}: {file}")
            pil_tensor, pil_hash, cv2_tensor, cv2_hash = self.generate_tensors(file)
            processed_data.append((str(file), pil_tensor.tobytes() if pil_tensor is not None else None, pil_hash, cv2_tensor.tobytes() if cv2_tensor is not None else None, cv2_hash))

        self.insert_into_table(conn, processed_data)
        self.db_conn_instance.return_connection(conn)
        print(f"Processing completed: {total_unmatched_files} unmatched files processed and inserted into the database.")

if __name__ == "__main__":
    validator = ValidationScript()
    validator.run()
