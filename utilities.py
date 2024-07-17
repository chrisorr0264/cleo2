'''
A container for various utility functions used throughout the cleo2 project.
2024 Christopher Orr
'''

from PIL import Image, UnidentifiedImageError
import pillow_heif
from pathlib import Path
from logger_config import get_logger
from time import time
import numpy as np
from glob import glob
import os
from settings import *
from dbconnection import DBConnection
import hashlib
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
import exiftool
import datetime as dt
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable
import socket
import requests
from wand.image import Image as WandImage
from wand.exceptions import WandException
import cv2
import subprocess
import json
import pwd
import shutil


class Utilities:
    def __init__(self):
        self.logger = get_logger(__name__)
        self.max_workers = 10
        self.db_conn_instance = DBConnection.get_instance()

    def get_new_files(self, directory):
        function_name = 'get_new_files'
        start_time = time()
        self.logger.debug(f"Getting new files from directory: {directory}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        
        if not os.path.isdir(directory):
            self.logger.error(f"Directory does not exist: {directory}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return [], []

        files = glob(os.path.join(directory, '*'))
        self.logger.debug(f"Files found: {files}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        
        if not files:
            self.logger.info("No files found in the directory", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return [], []

        valid_files_all = []
        skipped_files_all = []

        valid_files, skip_files = self.validate_files(files)
        valid_files_all.extend(valid_files)
        skipped_files_all.extend(skip_files)

        duration = time() - start_time
        self.logger.debug(f"Retrieved new files: {valid_files_all}, skipped files: {skipped_files_all}. Time taken: {duration:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        return valid_files_all, skipped_files_all

    def validate_files(self, files):
        function_name = 'validate_files'
        start_time = time()
        self.logger.debug(f"Validating files: {files}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        valid_files, skip_files = self.filter_extensions(files)
        duration = time() - start_time
        self.logger.debug(f"Validated files: {valid_files}, skipped files: {skip_files}. Time taken: {duration:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        return valid_files, skip_files

    def filter_extensions(self, files):
        function_name = 'filter_extensions'
        start_time = time()
        self.logger.debug(f"Filtering files by extension: {files}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        valid_image_extensions = set(IMAGE_EXTENSIONS)
        valid_movie_extensions = set(MOVIE_EXTENSIONS)
        keep_files = []
        skip_files = []

        for file in files:
            try:
                ext = file.split(".")[-1].lower()
                if ext in valid_image_extensions:
                    keep_files.append((file, 'image'))
                elif ext in valid_movie_extensions:
                    keep_files.append((file, 'movie'))
                else:
                    skip_files.append(file)

            except Exception as e:
                self.logger.error(f"Error processing file {file}: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                skip_files.append(file)

        duration = time() - start_time
        self.logger.debug(f"Filtered files: {keep_files}, skipped files: {skip_files}. Time taken: {duration:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        return keep_files, skip_files

    def move_to_error_directory(self, file):
        function_name = 'move_to_error_directory'
        try:
            if not os.path.exists(ERROR_DIRECTORY):
                os.makedirs(ERROR_DIRECTORY)
            shutil.move(file, os.path.join(ERROR_DIRECTORY, os.path.basename(file)))
            self.logger.info(f"Moved file {file} to error directory", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        except Exception as e:
            self.logger.error(f"Error moving file {file} to error directory: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

    def generate_tensor(self, file):
        function_name = 'generate_tensor'
        self.logger.debug(f"Generating tensor for file: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        try:
            file = self.check_and_convert_file(file)

            tensor_pil = self.generate_tensor_pil(file)
            tensor_cv2 = self.generate_tensor_cv2(file)

            if tensor_pil is not None and tensor_cv2 is not None:
                hash_pil = self.compute_tensor_hash(tensor_pil)
                hash_cv2 = self.compute_tensor_hash(tensor_cv2)

                self.logger.debug(f"Generated tensor for image file: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                return (file, tensor_pil, hash_pil, tensor_cv2, hash_cv2)
            else:
                self.logger.error(f"Failed to generate tensors for file: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                return None

        except UnidentifiedImageError:
            self.logger.error(f"UnidentifiedImageError: file {file} could not be identified as image.", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return {str(Path(file)): 'UnidentifiedImageError: file could not be identified as image.'}
        except Exception as e:
            self.logger.error(f"Error generating tensor for file {file}: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return {str(Path(file)): str(e)}

    def check_and_convert_file(self, file):
        function_name = 'check_and_convert_file'
        try:
            self.logger.info(f"Checking file type for: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            
            with open(file, 'rb') as f:
                file_header = f.read(10)
            
            actual_extension = None

            # List of possible ftyp sizes and corresponding HEIC/HEIF markers
            heic_markers = [
                b'ftypheic', b'ftypmif1', b'ftypmsf1', b'ftypheix', b'ftypheim', b'ftyphevc', b'ftyphe'
            ]
            heif_markers = [
                b'ftyphe', b'ftypmif1', b'ftypmsf1'
            ]
            
            # Check for HEIC markers with different sizes
            for size in [b'\x00\x00\x00\x18', b'\x00\x00\x00\x24', b'\x00\x00\x00\x28', b'\x00\x00\x00\x2C', b'\x00\x00\x00 ']:
                if any(file_header.startswith(size + marker) for marker in heic_markers):
                    actual_extension = '.heic'
                    break
                if any(file_header.startswith(size + marker) for marker in heif_markers):
                    actual_extension = '.heif'
                    break

            # Other image formats
            if file_header.startswith(b'\xff\xd8'):
                actual_extension = '.jpg'
            elif file_header.startswith(b'\x89PNG'):
                actual_extension = '.png'
            elif file_header.startswith(b'GIF87a') or file_header.startswith(b'GIF89a'):
                actual_extension = '.gif'
            elif file_header.startswith(b'BM'):
                actual_extension = '.bmp'
            elif file_header.startswith(b'\x00\x00\x01\x00'):
                actual_extension = '.ico'
            elif file_header.startswith(b'II*\x00') or file_header.startswith(b'MM\x00*'):
                actual_extension = '.tiff'
            # Other common formats
            elif file_header.startswith(b'\x25PDF'):
                actual_extension = '.pdf'
            elif file_header.startswith(b'\x50\x4B\x03\x04'):
                actual_extension = '.zip'
            elif file_header.startswith(b'\x52\x61\x72\x21'):
                actual_extension = '.rar'
            elif file_header.startswith(b'\x1F\x8B'):
                actual_extension = '.gz'
            elif file_header.startswith(b'\x42\x5A\x68'):
                actual_extension = '.bz2'
            elif file_header.startswith(b'PK'):
                actual_extension = '.docx'
            elif file_header.startswith(b'\xD0\xCF\x11\xE0'):
                actual_extension = '.doc'  # Could also be other older Microsoft Office formats like .xls or .ppt

            self.logger.detail(f'The actual extension based on bytes of file {file} should be {actual_extension}', extra={'class_name': self.__class__.__name__, 'function_name': function_name})            
            current_extension = os.path.splitext(file)[1].lower()
            
            if actual_extension and current_extension != actual_extension:
                new_file = os.path.splitext(file)[0] + actual_extension
                os.rename(file, new_file)
                self.logger.info(f"Renamed file to correct extension: {new_file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                file = new_file
            
            if actual_extension == '.heic':
                self.logger.info(f"Converting HEIC file to JPG: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                file = self.convert_heic_to_jpg(file)
            elif actual_extension in ['.png', '.gif']:
                self.logger.info(f"File is a PNG or GIF and will not be converted: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            elif actual_extension != '.jpg':
                self.logger.info(f"Converting non-JPG file to JPG: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                file = self.convert_to_jpg(file)
            
            return file
        except Exception as e:
            self.logger.error(f"Error checking or converting file type for {file}: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            raise

    def convert_heic_to_jpg(self, file):
        function_name = 'convert_heic_to_jpg'
        try:
            new_file = str(Path(file).with_suffix('.jpg'))
            with WandImage(filename=file) as img:
                img.format = 'jpeg'
                img.save(filename=new_file)
            self.logger.info(f"Converted HEIC {file} to JPG {new_file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})            
            
            # Remove the original HEIC file
            os.remove(file)
            self.logger.info(f"Deleted original HEIC file: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

            return new_file
        except WandException as e:
            self.logger.error(f"Error converting HEIC {file} to JPG: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            raise

    def convert_to_jpg(self, file):
        function_name = 'convert_to_jpg'
        try:
            with Image.open(file) as img:
                rgb_img = img.convert('RGB')
                new_file = str(Path(file).with_suffix('.jpg'))
                rgb_img.save(new_file, format='JPEG')
                self.logger.info(f"Converted {file} to {new_file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            
            # Remove the original file
            os.remove(file)
            self.logger.info(f"Deleted original file: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            
            return new_file
        except Exception as e:
            self.logger.error(f"Error converting {file} to JPG: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            raise

    def generate_tensor_pil(self, file):
        function_name = 'generate_tensor_pil'
        try:
            self.logger.info(f"Opening image with PIL: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            img = Image.open(file).convert('RGB')
            img = img.resize((50, 50), Image.BICUBIC)
            tensor = np.array(img)
            self.logger.debug(f"Generated tensor with PIL for: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return tensor
        except UnidentifiedImageError:
            error_message = f"Error: {file} is not a valid image."
            self.logger.error(error_message, extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return None
        except OSError as e:
            error_message = f"Error: {file} could not be processed with PIL. OSError: {e}"
            self.logger.error(error_message, extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return None

    def generate_tensor_cv2(self, file):
        function_name = 'generate_tensor_cv2'
        try:
            self.logger.info(f"Opening image with cv2: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            img = cv2.imread(file)
            if img is None:
                raise ValueError(f"cv2 could not open the image: {file}")
            img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            img = cv2.resize(img, (50, 50), interpolation=cv2.INTER_CUBIC)
            tensor = np.array(img)
            self.logger.debug(f"Generated tensor with cv2 for: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return tensor
        except Exception as e:
            error_message = f"Error: {file} could not be processed with cv2. Error: {e}"
            self.logger.error(error_message, extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return None

    def compute_tensor_hash(self, tensor):
        function_name = 'compute_tensor_hash'
        return hashlib.md5(tensor.tobytes()).hexdigest()
    
    def compute_mse(self, tensor_A, tensor_B, rotate=True):
        function_name = 'compute_mse'

        if rotate:
            mse_list = []
            for rot in range(4):
                mse = np.square(np.subtract(tensor_A, tensor_B)).mean()
                mse_list.append(mse)
                tensor_B = np.rot90(tensor_B)
            return min(mse_list)
        else:
            return np.square(np.subtract(tensor_A, tensor_B)).mean()
    
    def fetch_potential_duplicates(self, tensor_hash_pil, tensor_hash_cv2):
        function_name = 'fetch_potential_duplicates'
        try:
            conn = self.db_conn_instance.get_connection()
            cur = conn.cursor()

            select_query = """
            SELECT filename, tensor_pil, tensor_cv2, hash_pil, hash_cv2 
            FROM tbl_image_tensors
            WHERE hash_pil = %s OR hash_cv2 = %s
            """
            cur.execute(select_query, (tensor_hash_pil, tensor_hash_cv2))
            results = cur.fetchall()
            return results
        except Exception as e:
            self.logger.error(f"Failed to fetch potential duplicates: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return []
        finally:
            cur.close()
            self.db_conn_instance.return_connection(conn)

    def compare_with_potential_duplicates(self, tensor_pil, tensor_cv2, potential_duplicates, mse_threshold):
        function_name = 'compare_with_potential_duplicates'
        duplicates = []

        def compare_single(db_entry):
            db_filename, db_tensor_pil, db_tensor_cv2, db_hash_pil, db_hash_cv2 = db_entry
            required_shape = (50, 50, 3)

            try:
                if db_tensor_pil is not None:
                    db_tensor_pil = np.frombuffer(db_tensor_pil, dtype=tensor_pil.dtype)
                    if db_tensor_pil.size == np.prod(required_shape):
                        db_tensor_pil = db_tensor_pil.reshape(required_shape)
                        mse_pil = self.compute_mse(tensor_pil, db_tensor_pil)
                        if mse_pil <= mse_threshold:
                            return db_filename, mse_pil
                    else:
                        self.logger.error(f"Buffer size mismatch for db_tensor_pil: expected {np.prod(required_shape)}, got {db_tensor_pil.size}")

                if db_tensor_cv2 is not None:
                    db_tensor_cv2 = np.frombuffer(db_tensor_cv2, dtype=tensor_cv2.dtype)
                    if db_tensor_cv2.size == np.prod(required_shape):
                        db_tensor_cv2 = db_tensor_cv2.reshape(required_shape)
                        mse_cv2 = self.compute_mse(tensor_cv2, db_tensor_cv2)
                        if mse_cv2 <= mse_threshold:
                            return db_filename, mse_cv2
                    else:
                        self.logger.error(f"Buffer size mismatch for db_tensor_cv2: expected {np.prod(required_shape)}, got {db_tensor_cv2.size}")

            except Exception as e:
                self.logger.error(f"Error processing tensor for {db_filename}: {e}")

            return None

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_entry = {executor.submit(compare_single, entry): entry for entry in potential_duplicates}
            for future in as_completed(future_to_entry):
                result = future.result()
                if result:
                    duplicates.append(result)

        return duplicates
    
    def process_file(self, file_info):
        file, file_type = file_info
        if file_type == 'image':
            result = self.generate_tensor(file)
            if isinstance(result, tuple):
                filename, tensor = result
                tensor_hash = self.compute_tensor_hash(tensor)
                potential_duplicates = self.fetch_potential_duplicates(tensor_hash)
                duplicates = self.compare_with_potential_duplicates(filename, tensor, potential_duplicates, mse_threshold=0.01)
                if duplicates:
                    self.logger.info(f"Duplicates found for {filename}: {duplicates}")
                else:
                    self.insert_image_tensor(filename, tensor)
                    self.logger.info(f"Inserted {filename} into the database.")

    def move_file(self, old_file, new_file):
        function_name = 'move_file'

        self.logger.debug(f"Attempting to move file from {old_file} to {new_file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        try:
            if not os.path.exists(old_file):
                self.logger.error(f"Source file not found: {old_file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                return f"Source file not found: {old_file}"

            target_dir = os.path.dirname(new_file)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
                self.logger.debug(f"Created target directory: {target_dir}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

            os.replace(old_file, new_file)
            self.logger.debug(f"File moved successfully from {old_file} to {new_file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return "Success"
        except OSError as error:
            self.logger.error(f"Error moving file from {old_file} to {new_file}: {error}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return str(error)

    def get_image_metadata_from_file(self, path):
        function_name = 'get_image_metadata_from_file'
        self.logger.debug(f"Extracting metadata from image file: {path}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        try:
            with exiftool.ExifTool() as et:
                metadata = et.get_metadata(path)
            self.logger.detail(f"Image metadata extracted for {path}: {metadata}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return metadata
        except Exception as e:
            self.logger.error(f"Failed to extract metadata with EXIFtool for {path}: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return {}

    def get_file_create_date_for_image(self, file, metadata):
        function_name = 'get_file_create_date_for_image'
        self.logger.debug(f"Extracting file date for: {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        
        try:
            createDate = None
            if "EXIF:DateTimeOriginal" in metadata:
                date_str = metadata["EXIF:DateTimeOriginal"]
                date_format = '%Y:%m:%d %H:%M:%S'
                try:
                    createDate = dt.datetime.strptime(date_str, date_format)
                except ValueError:
                    createDate = None
            
            self.logger.debug(f"Extracted file create date: {file}, {createDate}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return createDate
        except KeyError as e:
            self.logger.error(f"Key error while extracting file create date: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return None

    def get_file_location_from_metadata(self, metadata, retries=3, delay=5, timeout=10, user_agent="locator"):
        function_name = 'get_file_location_from_metadata'

        self.logger.detail(f"Getting file location from metadata.", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        result = self.extract_key_values_containing_chars(metadata, "GPS")
        lat, long = self.get_lat_long(result)

        location_details = (None, None, None, None, None, None, None)

        if lat and long:
            geolocator = Nominatim(user_agent=user_agent)
            location = None
            for attempt in range(retries):
                try:
                    location = geolocator.reverse((lat, long), exactly_one=True, timeout=timeout)
                    break
                except GeocoderUnavailable:
                    self.logger.warning(f"GeocoderUnavailable: Attempt {attempt + 1} of {retries} failed. Retrying in {delay} seconds...", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                    if attempt < retries - 1:
                        time.sleep(delay)
                    else:
                        self.logger.error("Geocoding failed after multiple attempts.", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            if location:
                location_details = self.parse_location(location)
        else:
            self.logger.warning("No GPS data available in metadata", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        
        return (lat, long) + location_details

    def extract_key_values_containing_chars(self, dictionary, substring):
        function_name = 'extract_key_values_containing_chars'

        self.logger.detail(f"Extracting key values containing substring '{substring}' from dictionary", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        
        return {k: v for k, v in dictionary.items() if substring in k}

    def get_lat_long(self, metadata):
        function_name = 'get_lat_long'

        self.logger.debug(f"Getting latitude and longitude from metadata", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        try:
            latitude = float(metadata.get('EXIF:GPSLatitude', 0))
            longitude = float(metadata.get('EXIF:GPSLongitude', 0))
            if metadata.get('EXIF:GPSLatitudeRef', '') == 'S':
                latitude = -latitude
            if metadata.get('EXIF:GPSLongitudeRef', '') == 'W':
                longitude = -longitude
            if latitude and longitude:
                self.logger.detail(f"Retrieved latitude: {latitude}, longitude: {longitude}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                return latitude, longitude
        except ValueError as e:
            self.logger.error(f"Error converting GPS coordinates: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        self.logger.warning("No GPS coordinates found in metadata", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        return None, None

    def parse_location(self, location):
        function_name = 'parse_location'

        self.logger.detail(f"Parsing location data: {location.raw}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        location_class = location_type = location_name = location_display_name = location_city = location_province = location_country = None
        for key, value in location.raw.items():
            if key == 'class':
                location_class = value
            elif key == 'type':
                location_type = value
            elif key == 'name':
                location_name = value
            elif key == 'display_name':
                location_display_name = value
            elif key == 'address':
                location_address = value
                location_city = location_address.get('city')
                location_province = location_address.get('state')
                location_country = location_address.get('country')
        self.logger.debug(f"Retrieved location details: {location_class}, {location_type}, {location_name}, {location_display_name}, {location_city}, {location_province}, {location_country}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        return (
            location_class, 
            location_type, 
            location_name, 
            location_display_name, 
            location_city, 
            location_province, 
            location_country
        )
    
    def file_insert(self, orig_name, media_type):
        function_name = 'file_insert'

        self.logger.debug(f"Inserting file into database: {orig_name}, {media_type}", extra={'class_name': self.__class__.__name__,'function_name': function_name})
        
        query = """
        INSERT INTO tbl_media_objects (orig_name, media_type, created_by, created_ip)
        VALUES (%s, %s, %s, %s)
        RETURNING media_object_id
        """
        try:
            user = self.get_logged_in_user()
            hostname, ip = self.get_local_ip()
            
            conn = self.db_conn_instance.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (orig_name, media_type, user, ip))
            file_id = cursor.fetchone()[0]
            conn.commit()
            cursor.close()
            self.logger.debug(f"Inserted file into database with ID: {file_id}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            self.db_conn_instance.return_connection(conn)
            return file_id
        except Exception as e:
            self.logger.error(f"Error inserting file into database: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return None
        
    def get_local_ip(self):
        function_name = 'get_local_ip'

        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        return hostname, local_ip

    def get_external_ip(self):
        function_name = 'get_external_ip'

        try:
            external_ip = requests.get('https://api.ipify.org').text
        except requests.RequestException:
            external_ip='127.0.0.1'
        return external_ip

    def get_logged_in_user(self):
        function_name = 'get_logged_in_user'

        try:
            logged_in_user = pwd.getpwuid(os.geteuid()).pw_name
            return logged_in_user
        except Exception as e:
            self.logger.error(f"Error getting logged in user: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return 'Unknown User'
    
    def get_new_file_name(self, file_create_date, myID):
        function_name = 'get_new_file_name'

        self.logger.detail(f"Generating new file name with create date {file_create_date} and ID {myID}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        if file_create_date is None:
            date_part = "UnknownDate"
        else:
            try:
                if isinstance(file_create_date, dt.datetime):
                    create_date = file_create_date
                else:
                    create_date = dt.fromtimestamp(file_create_date)
                date_part = create_date.strftime('%Y-%m-%d')
            except Exception as e:
                self.logger.error(f"Error converting timestamp to date: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                date_part = "UnknownDate"
        
        new_file_name = f'{date_part}-{str(myID).zfill(7)}'
        self.logger.debug(f"Generated new file name: {new_file_name}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        return new_file_name

    def get_new_file_type(self, old_file_type):
        function_name = 'get_new_file_type'

        self.logger.debug(f"Determining new file type from old file type: {old_file_type}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                
        file_type = old_file_type.lower()
        if file_type == 'jpeg':
            new_file_type = 'jpg'
        else:
            new_file_type = file_type
        self.logger.debug(f"Determined new file type: {new_file_type}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        return new_file_type

    def file_update(self, new_name, new_path, file_create_date, lat, long, location_class, location_type, location_name, location_display_name, location_city, location_province, location_country, file_ID):
        function_name = 'file_update'  

        self.logger.debug(f"Updating file in database with ID {file_ID}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        
        query = """
        UPDATE tbl_media_objects
        SET new_name = %s, new_path = %s, media_create_date = %s, latitude = %s, longitude = %s, location_class = %s, location_type = %s, location_name = %s, location_display_name = %s, location_city = %s, location_province = %s, location_country = %s
        WHERE media_object_id = %s
        """
        try:
            conn = self.db_conn_instance.get_connection()
            cursor = conn.cursor()

            if file_create_date is not None:
                if isinstance(file_create_date, dt.datetime):
                    image_create_date = file_create_date
                else:
                    image_create_date = dt.fromtimestamp(file_create_date)
            else:
                image_create_date = None

            cursor.execute(query, (new_name, new_path, image_create_date, lat, long, location_class, location_type, location_name, location_display_name, location_city, location_province, location_country, file_ID))
            conn.commit()
            cursor.close()
            self.logger.debug(f"Updated file in database with ID {file_ID}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            self.db_conn_instance.return_connection(conn)
        except Exception as e:
            self.logger.error(f"Error updating file in database: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

    def flatten_dict(self, d, parent_key='', sep='_'):
        function_name = 'flatten_dict'

        if all(not isinstance(v, (dict, list)) for v in d.values()):
            return d

        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(self.flatten_dict(item, f"{new_key}{sep}{i}", sep=sep).items())
                    else:
                        items.append((f"{new_key}{sep}{i}", item))
            else:
                items.append((new_key, v))
        return dict(items)

    def insert_metadata(self, metadata, file_ID):
        function_name = 'insert_metadata'

        self.logger.debug(f"Inserting metadata for file ID {file_ID}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        conn = self.db_conn_instance.get_connection()
        try:
            with conn.cursor() as cursor:
                sql_statement = "INSERT INTO tbl_media_metadata (media_object_id, exif_tag, exif_data) VALUES (%s, %s, %s)"
                for exif_tag, exif_data in metadata.items():
                    if isinstance(exif_data, list):
                        exif_data = self.convert_list_to_string(exif_data)
                    cursor.execute(sql_statement, (file_ID, exif_tag, exif_data))
                conn.commit()
                self.logger.debug(f"Inserted metadata for file ID {file_ID}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        except Exception as e:
            self.logger.error(f"Error inserting metadata for file ID {file_ID}: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        finally:
            self.db_conn_instance.return_connection(conn)

    def convert_list_to_string(self, li):
        function_name = 'convert_list_to_string'

        self.logger.debug(f"Converting list to string: {li}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        s = ' '.join([str(elem) for elem in li])
        return s

    def insert_image_tensor(self, file, tensor_pil, hash_pil, tensor_cv2, hash_cv2, media_object_id):
        function_name = 'insert_image_tensor'
        try:
            conn = self.db_conn_instance.get_connection()
            cur = conn.cursor()

            required_shape = (50, 50, 3)
            tensor_shape_pil = tensor_pil.shape if tensor_pil is not None else None
            tensor_shape_cv2 = tensor_cv2.shape if tensor_cv2 is not None else None

            if tensor_shape_pil != required_shape or tensor_shape_cv2 != required_shape:
                self.logger.error(f"Invalid tensor shape. Expected {required_shape} but got {tensor_shape_pil} and {tensor_shape_cv2}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                raise ValueError(f"Invalid tensor shape. Expected {required_shape} but got {tensor_shape_pil} and {tensor_shape_cv2}")

            tensor_pil_bytes = tensor_pil.tobytes() if tensor_pil is not None else None
            tensor_cv2_bytes = tensor_cv2.tobytes() if tensor_cv2 is not None else None

            if tensor_pil_bytes is not None and len(tensor_pil_bytes) != np.prod(required_shape):
                self.logger.error(f"Invalid tensor_pil byte size. Expected {np.prod(required_shape)} but got {len(tensor_pil_bytes)}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                raise ValueError(f"Invalid tensor_pil byte size. Expected {np.prod(required_shape)} but got {len(tensor_pil_bytes)}")
            if tensor_cv2_bytes is not None and len(tensor_cv2_bytes) != np.prod(required_shape):
                self.logger.error(f"Invalid tensor_cv2 byte size. Expected {np.prod(required_shape)} but got {len(tensor_cv2_bytes)}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                raise ValueError(f"Invalid tensor_cv2 byte size. Expected {np.prod(required_shape)} but got {len(tensor_cv2_bytes)}")

            insert_query = """
            INSERT INTO tbl_image_tensors (filename, tensor_pil, tensor_cv2, hash_pil, hash_cv2, tensor_shape)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """
            
            cur.execute(insert_query, (
                file,
                tensor_pil_bytes,
                tensor_cv2_bytes,
                hash_pil,
                hash_cv2,
                str(required_shape)
            ))

            tensor_id = cur.fetchone()[0]

            update_query = """
            UPDATE tbl_media_objects
            SET image_tensor_id = %s
            WHERE media_object_id = %s
            """
            
            cur.execute(update_query, (tensor_id, media_object_id))
            conn.commit()

            return tensor_id

        except Exception as e:
            self.logger.error(f"Failed to insert image tensor: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            conn.rollback()
            raise
        finally:
            cur.close()
            self.db_conn_instance.return_connection(conn)
    
    def generate_movie_hash(self, file_path):
        function_name = 'generate_movie_hash'
        self.logger.debug(f"Generating hash for movie file: {file_path}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        try:
            hasher = hashlib.md5()
            with open(file_path, 'rb') as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)
            movie_hash = hasher.hexdigest()
            self.logger.debug(f"Generated hash for movie file: {movie_hash}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return (file_path, movie_hash)
        except Exception as e:
            self.logger.error(f"Error generating hash for movie file {file_path}: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return None

    def fetch_potential_movie_duplicates(self, movie_hash):
        function_name = 'fetch_potential_movie_duplicates'
        try:
            conn = self.db_conn_instance.get_connection()
            cur = conn.cursor()

            select_query = """
            SELECT filename, media_hash 
            FROM tbl_movie_hashes
            WHERE media_hash = %s
            """
            cur.execute(select_query, (movie_hash,))
            results = cur.fetchall()
            return results
        except Exception as e:
            self.logger.error(f"Failed to fetch potential movie duplicates: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return []
        finally:
            cur.close()
            self.db_conn_instance.return_connection(conn)

    def insert_movie_hash(self, file_path, movie_hash, media_object_id):
        function_name = 'insert_movie_hash'
        try:
            conn = self.db_conn_instance.get_connection()
            cur = conn.cursor()

            insert_query = """
            INSERT INTO tbl_movie_hashes (filename, media_hash)
            VALUES (%s, %s)
            RETURNING id
            """
            
            cur.execute(insert_query, (file_path, movie_hash))
            movie_hash_id = cur.fetchone()[0]

            update_query = """
            UPDATE tbl_media_objects
            SET movie_hash_id = %s
            WHERE media_object_id = %s
            """
            
            cur.execute(update_query, (movie_hash_id, media_object_id))
            conn.commit()

            return movie_hash_id

        except Exception as e:
            self.logger.error(f"Failed to insert movie hash: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            conn.rollback()
            raise
        finally:
            cur.close()
            self.db_conn_instance.return_connection(conn)

    def get_movie_metadata_from_file(self, path):
        function_name = 'get_movie_metadata_from_file'
        self.logger.debug(f"Extracting metadata from movie file: {path}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        try:
            cmd = [
                'ffprobe', '-v', 'error', '-print_format', 'json', '-show_format', '-show_streams', path
            ]
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            metadata = json.loads(result.stdout)
            self.logger.detail(f"Movie metadata extracted for {path}: {metadata}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return metadata
        except Exception as e:
            self.logger.error(f"Failed to extract metadata with ffprobe for {path}: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return {}

    def get_file_create_date_for_movie(self, file, metadata):
        function_name = 'get_file_create_date_for_movie'
        self.logger.debug(f"Extracting file date from metadata: {metadata}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        
        try:
            createDate = None
            if "format" in metadata and "tags" in metadata["format"] and "creation_time" in metadata["format"]["tags"]:
                date_str = metadata["format"]["tags"]["creation_time"]
                try:
                    createDate = dt.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                except ValueError:
                    createDate = None
            
            self.logger.debug(f"Extracted filename data: {file}, {createDate}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return createDate
        except KeyError as e:
            self.logger.error(f"Key error while extracting filename data: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return None
        
    def get_file_location_from_movie_metadata(self, metadata):
        function_name = 'get_file_location_from_movie_metadata'
        self.logger.debug(f"Extracting location data from metadata: {metadata}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        
        latitude = None
        longitude = None
        location_details = (None, None, None, None, None, None, None)

        try:
            if "streams" in metadata:
                for stream in metadata["streams"]:
                    if "tags" in stream:
                        if "location" in stream["tags"]:
                            location_str = stream["tags"]["location"]
                            # Assuming location is in the format "+37.3861-122.0839/" (latitude+longitude)
                            if location_str.startswith('+') or location_str.startswith('-'):
                                lat_str, long_str = location_str.split('-')
                                latitude = float(lat_str)
                                longitude = -float(long_str)
                                break
            
            if latitude and longitude:
                location_details = self.get_location_from_coordinates(latitude, longitude)
        except Exception as e:
            self.logger.error(f"Error extracting location data: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        return (latitude, longitude) + location_details

    def get_location_from_coordinates(self, lat, long, retries=3, delay=5, timeout=10, user_agent="movie_locator"):
        function_name = 'get_location_from_coordinates'
        
        self.logger.detail(f"Getting location from coordinates: {lat}, {long}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        location_class = location_type = location_name = location_display_name = location_city = location_province = location_country = None

        geolocator = Nominatim(user_agent=user_agent)
        location = None
        for attempt in range(retries):
            try:
                location = geolocator.reverse((lat, long), exactly_one=True, timeout=timeout)
                break
            except GeocoderUnavailable:
                self.logger.warning(f"GeocoderUnavailable: Attempt {attempt + 1} of {retries} failed. Retrying in {delay} seconds...", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    self.logger.error("Geocoding failed after multiple attempts.", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        if location:
            location_class, location_type, location_name, location_display_name, location_city, location_province, location_country = self.parse_location(location)
        
        return (location_class, location_type, location_name, location_display_name, location_city, location_province, location_country)
