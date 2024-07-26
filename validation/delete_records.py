import sys
sys.path.append('/opt/cleo')
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
import re

load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_SERVER')
DB_PORT = os.getenv('DB_PORT')

# Function to read media_object_ids from the updated_report.txt file
def read_media_object_ids(file_path):
    media_object_ids = []
    with open(file_path, 'r') as file:
        for line in file:
            match = re.match(r'\((\d+),', line)
            if match:
                media_object_id = int(match.group(1))
                media_object_ids.append(media_object_id)
    return media_object_ids

# Path to the updated_report.txt file
report_file_path = '/opt/cleo/updated_report.txt'

# Read the media_object_ids from the report file
media_object_ids = read_media_object_ids(report_file_path)

if not media_object_ids:
    print("No valid media_object_id found in the report file.")
else:
    print(f"Found {len(media_object_ids)} media_object_ids to delete.")

    # Connect to the database
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    try:
        with conn.cursor() as cursor:
            print("Creating temporary table to hold media_object_ids...")
            # Create a temporary table to hold the ids to be deleted
            cursor.execute("""
                CREATE TEMP TABLE temp_media_object_ids (
                    media_object_id INT PRIMARY KEY
                )
            """)

            print("Inserting media_object_ids into the temporary table...")
            # Insert the ids into the temporary table
            execute_values(
                cursor,
                "INSERT INTO temp_media_object_ids (media_object_id) VALUES %s",
                [(id,) for id in media_object_ids]
            )

            print("Deleting related records from tbl_media_metadata...")
            # Delete related records from tbl_media_metadata
            cursor.execute("""
                DELETE FROM tbl_media_metadata
                WHERE media_object_id IN (SELECT media_object_id FROM temp_media_object_ids)
            """)

            print("Deleting related records from tbl_tags_to_media...")
            # Delete related records from tbl_tags_to_media
            cursor.execute("""
                DELETE FROM tbl_tags_to_media
                WHERE media_object_id IN (SELECT media_object_id FROM temp_media_object_ids)
            """)

            print("Deleting related records from tbl_identified_faces...")
            # Delete related records from tbl_identified_faces
            cursor.execute("""
                DELETE FROM tbl_identified_faces
                WHERE media_object_id IN (SELECT media_object_id FROM temp_media_object_ids)
            """)

            print("Deleting related records from tbl_invalid_faces...")
            # Delete related records from tbl_invalid_faces
            cursor.execute("""
                DELETE FROM tbl_invalid_faces
                WHERE media_object_id IN (SELECT media_object_id FROM temp_media_object_ids)
            """)

            print("Deleting records from tbl_media_objects...")
            # Delete records from tbl_media_objects
            cursor.execute("""
                DELETE FROM tbl_media_objects
                WHERE media_object_id IN (SELECT media_object_id FROM temp_media_object_ids)
            """)

        # Commit the transaction
        conn.commit()
        print("Records deleted successfully.")
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()
