import os
import psycopg2
import hashlib
import numpy as np
from PIL import Image, ImageFile
import cv2
from pathlib import Path
from psycopg2.extras import execute_values

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_SERVER')
DB_PORT = os.getenv('DB_PORT')

DUPLICATE_DIR = '/mnt/MOM/Duplicates'
IMAGE_DIR = '/mnt/MOM/Images'
MOVIE_DIR = '/mnt/MOM/Movies'
NEW_FOLDER = '/mnt/MOM/New'

ImageFile.LOAD_TRUNCATED_IMAGES = True

def compute_tensor_hash(tensor):
    return hashlib.md5(tensor.tobytes()).hexdigest()

def generate_tensor_pil(file_path):
    try:
        img = Image.open(file_path).convert('RGB')
        img = img.resize((50, 50), Image.BICUBIC)
        tensor = np.array(img)
        return tensor
    except Exception as e:
        print(f"Error processing PIL tensor for {file_path}: {e}")
        return None

def generate_tensor_cv2(file_path):
    try:
        img = cv2.imread(file_path)
        if img is None:
            raise ValueError(f"cv2 could not open the image: {file_path}")
        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        img = cv2.resize(img, (50, 50), interpolation=cv2.INTER_CUBIC)
        tensor = np.array(img)
        return tensor
    except Exception as e:
        print(f"Error processing cv2 tensor for {file_path}: {e}")
        return None

def fix_image(file_path):
    try:
        img = Image.open(file_path)
        img.save(file_path)
        return True
    except Exception as e:
        print(f"Error fixing image {file_path}: {e}")
        return False

def insert_into_duplicate_tables(conn, duplicate_dir):
    with conn.cursor() as cursor:
        # Process duplicate images
        print("Processing duplicate images...")
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp']
        image_files = [file for ext in image_extensions for file in Path(duplicate_dir).rglob(f'*{ext}')]
        image_values = []
        for idx, file_path in enumerate(image_files):
            print(f"Processing image {idx + 1}/{len(image_files)}: {file_path}")
            pil_tensor = generate_tensor_pil(file_path)
            cv2_tensor = generate_tensor_cv2(file_path)
            if pil_tensor is None or cv2_tensor is None:
                print(f"Attempting to fix image: {file_path}")
                if fix_image(file_path):
                    pil_tensor = generate_tensor_pil(file_path)
                    cv2_tensor = generate_tensor_cv2(file_path)
            if pil_tensor is not None and cv2_tensor is not None:
                pil_hash = compute_tensor_hash(pil_tensor)
                cv2_hash = compute_tensor_hash(cv2_tensor)
                image_values.append((str(file_path), str(pil_tensor.shape), pil_tensor.tobytes(), pil_hash, cv2_tensor.tobytes(), cv2_hash))
        if image_values:
            execute_values(cursor, """
                INSERT INTO tbl_duplicate_images (filename, tensor_shape, tensor_pil, hash_pil, tensor_cv2, hash_cv2)
                VALUES %s
            """, image_values)

        # Process duplicate movies
        print("Processing duplicate movies...")
        movie_extensions = ['.mov', '.MOV', '.avi', '.AVI', '.MTS', '.mts', '.mp4', '.MP4']
        movie_files = [file for ext in movie_extensions for file in Path(duplicate_dir).rglob(f'*{ext}')]
        movie_values = []
        for idx, file_path in enumerate(movie_files):
            print(f"Processing movie {idx + 1}/{len(movie_files)}: {file_path}")
            media_hash = hashlib.md5(file_path.read_bytes()).hexdigest()
            movie_values.append((str(file_path), media_hash))
        if movie_values:
            execute_values(cursor, """
                INSERT INTO tbl_duplicate_movies (filename, media_hash)
                VALUES %s
            """, movie_values)
        conn.commit()

def validate_duplicates(conn, duplicate_dir, new_folder):
    with conn.cursor() as cursor:
        # Validate duplicate images
        print("Validating duplicate images...")
        cursor.execute("""
            SELECT di.filename, di.hash_pil, di.hash_cv2, it.hash_pil, it.hash_cv2 
            FROM tbl_duplicate_images di
            LEFT JOIN tbl_image_tensors it ON di.hash_pil = it.hash_pil OR di.hash_cv2 = it.hash_cv2
        """)
        duplicate_images = cursor.fetchall()
        total_images = len(duplicate_images)
        unvalidated_images = []
        for idx, dup_image in enumerate(duplicate_images):
            print(f"Validating image {idx + 1}/{total_images}: {dup_image[0]}")
            if dup_image[3] is None and dup_image[4] is None:
                new_path = Path(new_folder) / Path(dup_image[0]).name
                os.rename(dup_image[0], new_path)
                unvalidated_images.append(dup_image[0])

        # Validate duplicate movies
        print("Validating duplicate movies...")
        cursor.execute("""
            SELECT dm.filename, dm.media_hash, mh.media_hash 
            FROM tbl_duplicate_movies dm
            LEFT JOIN tbl_movie_hashes mh ON dm.media_hash = mh.media_hash
        """)
        duplicate_movies = cursor.fetchall()
        total_movies = len(duplicate_movies)
        unvalidated_movies = []
        for idx, dup_movie in enumerate(duplicate_movies):
            print(f"Validating movie {idx + 1}/{total_movies}: {dup_movie[0]}")
            if dup_movie[2] is None:
                new_path = Path(new_folder) / Path(dup_movie[0]).name
                os.rename(dup_movie[0], new_path)
                unvalidated_movies.append(dup_movie[0])
        conn.commit()
        
        return len(duplicate_images), len(duplicate_movies), len(unvalidated_images), len(unvalidated_movies)

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    try:
        insert_into_duplicate_tables(conn, DUPLICATE_DIR)
        total_images, total_movies, unvalidated_images, unvalidated_movies = validate_duplicates(conn, DUPLICATE_DIR, NEW_FOLDER)
        
        print(f"Total duplicate images processed: {total_images}")
        print(f"Total duplicate movies processed: {total_movies}")
        print(f"Total unvalidated images moved to new folder: {unvalidated_images}")
        print(f"Total unvalidated movies moved to new folder: {unvalidated_movies}")
        
        if unvalidated_images == 0 and unvalidated_movies == 0:
            print("All duplicates have been successfully validated.")
        else:
            print("Some duplicates could not be validated and were moved to the new folder.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
