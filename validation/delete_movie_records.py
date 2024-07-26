import psycopg2
import pathlib
import os
from dotenv import load_dotenv

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
            SELECT media_object_id, new_name, movie_hash_id 
            FROM public.tbl_media_objects 
            WHERE is_active = TRUE AND media_type = 'movie'
        """)
        return cursor.fetchall()

def get_files_in_directory(movie_dir):
    movie_dir_path = pathlib.Path(movie_dir)
    return set(movie_dir_path.rglob('*'))

def delete_unmatched_movies(conn, db_only_files):
    with conn.cursor() as cursor:
        for media_object_id, _, movie_hash_id in db_only_files:
            try:
                if movie_hash_id is not None:
                    cursor.execute("BEGIN;")
                    cursor.execute("DELETE FROM tbl_movie_hashes WHERE id = %s;", (movie_hash_id,))
                    cursor.execute("DELETE FROM tbl_media_objects WHERE media_object_id = %s;", (media_object_id,))
                    cursor.execute("COMMIT;")
                else:
                    cursor.execute("BEGIN;")
                    cursor.execute("DELETE FROM tbl_media_objects WHERE media_object_id = %s;", (media_object_id,))
                    cursor.execute("COMMIT;")
                print(f"Deleted records for media_object_id: {media_object_id}")
            except Exception as e:
                cursor.execute("ROLLBACK;")
                print(f"Error deleting records for media_object_id: {media_object_id}, error: {e}")

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
        db_files = {pathlib.Path(MOVIE_DIR) / new_name: (media_object_id, new_name, movie_hash_id) for media_object_id, new_name, movie_hash_id in db_entries}

        dir_files = get_files_in_directory(MOVIE_DIR)

        # Files in DB but not in directory
        db_only_files = {db_files[file_path] for file_path in db_files.keys() if file_path not in dir_files}

        delete_unmatched_movies(conn, db_only_files)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
