import os
import sys
import logging
import json
import cv2
from PIL import Image
import numpy as np
from dotenv import load_dotenv
from pathlib import Path
from dbconnection import DBConnection
from logger_config import setup_logging, get_logger
from utilities import Utilities
from tqdm import tqdm
import concurrent.futures

# Load environment variables from .env file
load_dotenv()

IMAGE_DIRECTORY = '/mnt/MOM/Images'
ERROR_DIRECTORY = '/mnt/MOM/Errors'

class ValidationScript:
    def __init__(self):
        setup_logging()
        self.logger = get_logger('validation')
        self.image_directory = Path(IMAGE_DIRECTORY)
        self.error_directory = Path(ERROR_DIRECTORY)
        self.db_conn_instance = DBConnection.get_instance()
        self.util = Utilities()

    def get_db_images(self, batch_size=1000, offset=0):
        query = "SELECT media_object_id, new_name, new_path, image_tensor_id FROM tbl_media_objects WHERE media_type = 'image' LIMIT %s OFFSET %s"
        connection = self.db_conn_instance.get_connection()
        cursor = connection.cursor()
        cursor.execute(query, (batch_size, offset))
        images = cursor.fetchall()
        cursor.close()
        self.db_conn_instance.return_connection(connection)
        return images

    def get_db_tensors(self, image_tensor_id):
        query = "SELECT tensor_pil, tensor_cv2 FROM tbl_image_tensors WHERE id = %s"
        connection = self.db_conn_instance.get_connection()
        cursor = connection.cursor()
        cursor.execute(query, (image_tensor_id,))
        tensors = cursor.fetchone()
        cursor.close()
        self.db_conn_instance.return_connection(connection)
        return tensors

    def phase1_check_files(self):
        batch_size = 1000
        offset = 0
        discrepancies = []

        directory_files = list(self.image_directory.glob('**/*'))
        directory_files_dict = {file: None for file in directory_files}

        while True:
            db_images = self.get_db_images(batch_size, offset)
            if not db_images:
                break

            for image in db_images:
                if image[1] and image[2]:  # Check if new_name and new_path are not None
                    db_file = Path(image[2]) / image[1]
                    if db_file not in directory_files_dict:
                        error_file = self.error_directory / image[1]
                        if error_file.exists():
                            self.logger.info(f"File found in error directory: {error_file}")
                            discrepancies.append((str(db_file), "File in error directory"))
                        else:
                            discrepancies.append((str(db_file), "File not found in directory or error directory"))

            offset += batch_size
            self.logger.info(f"Processed {offset} entries from database")

        self.generate_phase1_report(discrepancies)
        return discrepancies

    def phase2_extended_checks(self):
        correct_files = []
        discrepancies = []

        directory_files = list(self.image_directory.glob('**/*'))
        directory_files_dict = {file: None for file in directory_files}

        batch_size = 1000
        offset = 0

        while True:
            db_images = self.get_db_images(batch_size, offset)
            if not db_images:
                break

            db_images_dict = {
                Path(image[2]) / image[1]: image
                for image in db_images if image[2] and image[1]
            }

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_file = {
                    executor.submit(self.validate_file, file, db_images_dict.get(file)): file
                    for file in directory_files if file in db_images_dict
                }

                for future in tqdm(concurrent.futures.as_completed(future_to_file), total=len(future_to_file), desc="Validating images"):
                    file = future_to_file[future]
                    try:
                        result = future.result()
                        if result:
                            correct_files.append((str(file), self.get_db_tensors(db_images_dict[file][3])))
                        else:
                            discrepancies.append((str(file), "Missing tensors in database or validation failed"))
                    except Exception as e:
                        discrepancies.append((str(file), f"Exception: {e}"))

            offset += batch_size
            self.logger.info(f"Processed {offset} entries from database")

        self.generate_phase2_report(correct_files, discrepancies)
        return correct_files, discrepancies

    def validate_file(self, file, db_image):
        if not db_image:
            return False

        media_object_id = db_image[0]
        image_tensor_id = db_image[3]
        if not image_tensor_id:
            return False

        tensors = self.get_db_tensors(image_tensor_id)
        if not tensors:
            self.logger.error(f"No tensors found for image_tensor_id: {image_tensor_id}")
            return False

        tensor_pil, tensor_cv2 = tensors
        try:
            pil_image = Image.open(file).convert('RGB')
            pil_tensor = np.array(pil_image.resize((50, 50), Image.BICUBIC))
            cv2_image = cv2.imread(str(file))
            cv2_tensor = cv2.resize(cv2.cvtColor(cv2_image, cv2.COLOR_BGR2RGB), (50, 50))

            if not np.array_equal(pil_tensor, tensor_pil):
                self.logger.error(f"PIL tensor mismatch for file: {file}")
                return False

            if not np.array_equal(cv2_tensor, tensor_cv2):
                self.logger.error(f"CV2 tensor mismatch for file: {file}")
                return False

            return True
        except Exception as e:
            self.logger.error(f"Failed to validate file {file}: {e}")
            return False

    def generate_phase1_report(self, discrepancies):
        report = {
            "total_discrepancies": len(discrepancies),
            "discrepancy_details": discrepancies
        }

        with open("phase1_report.json", "w") as f:
            json.dump(report, f, indent=4)
        self.logger.info("Phase 1 validation report generated.")

    def generate_phase2_report(self, correct_files, discrepancies):
        report = {
            "total_correct_files": len(correct_files),
            "files_with_face_identified": 0,
            "files_with_metadata_recorded": 0,
            "files_with_complete_metadata": 0,
            "files_with_valid_conversions": 0,
            "total_discrepancies": len(discrepancies),
            "discrepancy_details": discrepancies
        }

        # Check for face identification and metadata
        for file, tensors in correct_files:
            media_object_id = self.get_media_object_id(file)
            if self.has_face_identified(media_object_id):
                report["files_with_face_identified"] += 1
            if self.has_metadata_recorded(media_object_id):
                report["files_with_metadata_recorded"] += 1

        with open("phase2_report.json", "w") as f:
            json.dump(report, f, indent=4)
        self.logger.info("Phase 2 validation report generated.")

    def get_media_object_id(self, file):
        query = "SELECT media_object_id FROM tbl_media_objects WHERE new_name = %s AND new_path = %s"
        connection = self.db_conn_instance.get_connection()
        cursor = connection.cursor()
        cursor.execute(query, (file.name, str(file.parent)))
        media_object_id = cursor.fetchone()
        cursor.close()
        self.db_conn_instance.return_connection(connection)
        return media_object_id[0] if media_object_id else None

    def has_face_identified(self, media_object_id):
        query = "SELECT COUNT(*) FROM tbl_identified_faces WHERE media_object_id = %s"
        connection = self.db_conn_instance.get_connection()
        cursor = connection.cursor()
        cursor.execute(query, (media_object_id,))
        count = cursor.fetchone()[0]
        cursor.close()
        self.db_conn_instance.return_connection(connection)
        return count > 0

    def has_metadata_recorded(self, media_object_id):
        query = "SELECT COUNT(*) FROM tbl_media_metadata WHERE media_object_id = %s"
        connection = self.db_conn_instance.get_connection()
        cursor = connection.cursor()
        cursor.execute(query, (media_object_id,))
        count = cursor.fetchone()[0]
        cursor.close()
        self.db_conn_instance.return_connection(connection)
        return count > 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python validation.py <phase>")
        print("phase: 'phase1' to check file existence, 'phase2' for extended checks")
        sys.exit(1)

    phase = sys.argv[1]
    validator = ValidationScript()

    if phase == "phase1":
        discrepancies = validator.phase1_check_files()
        print(f"Phase 1 completed with {len(discrepancies)} discrepancies.")
    elif phase == "phase2":
        correct_files, discrepancies = validator.phase2_extended_checks()
        print(f"Phase 2 completed with {len(correct_files)} correct files and {len(discrepancies)} discrepancies.")
    else:
        print("Invalid phase specified. Use 'phase1' or 'phase2'.")
