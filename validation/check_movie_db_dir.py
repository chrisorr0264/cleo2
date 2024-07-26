import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
import pathlib

load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_SERVER')
DB_PORT = os.getenv('DB_PORT')

MOVIE_DIR = '/mnt/MOM/Movies'  # Directory where your movie files are stored

def get_db_entries(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT media_object_id, new_name 
            FROM public.tbl_media_objects 
            WHERE is_active = TRUE AND media_type = 'movie'
        """)
        return cursor.fetchall()

def get_files_in_directory(movie_dir):
    movie_dir_path = pathlib.Path(movie_dir)
    return list(movie_dir_path.rglob('*'))

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    try:
        db_entries = get_db_entries(conn)
        db_files = {pathlib.Path(MOVIE_DIR) / new_name: media_object_id for media_object_id, new_name in db_entries}

        dir_files = set(get_files_in_directory(MOVIE_DIR))

        # Files in DB but not in directory
        db_only_files = set(db_files.keys()) - dir_files

        # Files in directory but not in DB
        dir_only_files = dir_files - set(db_files.keys())

        with open('/opt/cleo/validation/db_only_movie_files.txt', 'w') as db_file_report:
            db_file_report.write(f"Movies in database but not in directory: {len(db_only_files)}\n")
            for file_path in db_only_files:
                db_file_report.write(f"{db_files[file_path]}, {file_path}\n")

        with open('/opt/cleo/validation/dir_only_movie_files.txt', 'w') as dir_file_report:
            dir_file_report.write(f"Movies in directory but not in database: {len(dir_only_files)}\n")
            for file_path in dir_only_files:
                dir_file_report.write(f"{file_path}\n")

        print("Movie reports generated successfully.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()