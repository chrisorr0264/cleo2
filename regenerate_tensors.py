import os
import numpy as np
from PIL import Image, UnidentifiedImageError, ImageFile
import cv2
import hashlib
import psycopg2
from dbconnection import get_connection, return_connection

# This is required to handle truncated image files
ImageFile.LOAD_TRUNCATED_IMAGES = True

def generate_tensor_pil(file_path):
    try:
        print(f"Opening image with PIL: {file_path}")
        img = Image.open(file_path).convert('RGB')
        img = img.resize((50, 50), Image.BICUBIC)
        tensor = np.array(img)
        print(f"Generated tensor with PIL for: {file_path}")
        return tensor
    except UnidentifiedImageError:
        error_message = f"Error: {file_path} is not a valid image."
        print(error_message)
        return None
    except OSError as e:
        error_message = f"Error: {file_path} could not be processed with PIL. OSError: {e}"
        print(error_message)
        return None

def generate_tensor_cv2(file_path):
    try:
        print(f"Opening image with cv2: {file_path}")
        img = cv2.imread(file_path)
        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        img = cv2.resize(img, (50, 50), interpolation=cv2.INTER_CUBIC)
        tensor = np.array(img)
        print(f"Generated tensor with cv2 for: {file_path}")
        return tensor
    except Exception as e:
        error_message = f"Error: {file_path} could not be processed with cv2. Error: {e}"
        print(error_message)
        return None

def compute_tensor_hash(tensor):
    hash_value = hashlib.md5(tensor.tobytes()).hexdigest()
    print(f"Computed hash: {hash_value}")
    return hash_value

def insert_tensor_data(cursor, filename, tensor_pil, hash_pil, tensor_cv2, hash_cv2, tensor_shape):
    query = """
    INSERT INTO image_tensors (filename, tensor_pil, hash_pil, tensor_cv2, hash_cv2, tensor_shape)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (filename, psycopg2.Binary(tensor_pil.tobytes()), hash_pil,
                           psycopg2.Binary(tensor_cv2.tobytes()), hash_cv2, tensor_shape))
    print(f"Inserted data for file: {filename}")

def compare_tensors(file_path, log, cursor):
    print(f"Comparing tensors for file: {file_path}")
    tensor_pil = generate_tensor_pil(file_path)
    tensor_cv2 = generate_tensor_cv2(file_path)

    if tensor_pil is not None and tensor_cv2 is not None:
        hash_pil = compute_tensor_hash(tensor_pil)
        hash_cv2 = compute_tensor_hash(tensor_cv2)

        log.write(f"Hashes for {file_path}:\n")
        log.write(f"PIL Hash: {hash_pil}\n")
        log.write(f"cv2 Hash: {hash_cv2}\n")

        if hash_pil == hash_cv2:
            result = "The tensors are identical."
        else:
            result = "The tensors are different."
        log.write(result + "\n")
        print(result)

        tensor_shape = str(tensor_pil.shape)
        insert_tensor_data(cursor, os.path.basename(file_path), tensor_pil, hash_pil, tensor_cv2, hash_cv2, tensor_shape)
    else:
        result = f"Could not process {file_path} with both libraries."
        log.write(result + "\n")
        print(result)

# Function to alter the table structure if necessary
def alter_table_structure(cursor):
    print("Altering the image_tensors table to add new columns")
    cursor.execute("""
    ALTER TABLE image_tensors
    ADD COLUMN IF NOT EXISTS tensor_pil BYTEA,
    ADD COLUMN IF NOT EXISTS hash_pil TEXT,
    ADD COLUMN IF NOT EXISTS tensor_cv2 BYTEA,
    ADD COLUMN IF NOT EXISTS hash_cv2 TEXT,
    ADD COLUMN IF NOT EXISTS tensor_shape TEXT;
    """)
    conn.commit()
    print("Table altered")

# Specify the directory and log file path
directory = 'M:\\Images'
log_file_path = 'tensor_comparison_results.txt'

# Open the log file
with open(log_file_path, 'w') as log:
    print("Connecting to the database")
    conn = get_connection()
    cursor = conn.cursor()
    
    # Alter the table structure if necessary
    alter_table_structure(cursor)
    
    # Get total number of files for progress tracking
    total_files = len([name for name in os.listdir(directory) if os.path.isfile(os.path.join(directory, name))])
    processed_files = 0
    print(f"Total files to process: {total_files}")
    
    # Process and compare tensors for each image in the directory
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            processed_files += 1
            log.write(f"Processing file: {filename}\n")
            print(f"Processing file {processed_files}/{total_files}: {filename}")
            compare_tensors(file_path, log, cursor)
    
    conn.commit()
    cursor.close()
    return_connection(conn)
    print("Database connection closed")

print(f"Results have been written to {log_file_path}")
