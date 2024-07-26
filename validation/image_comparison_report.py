import os
import csv
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

IMAGE_DIRECTORY = '/mnt/MOM/Images'

def get_db_images():
    query = "SELECT new_name, new_path FROM tbl_media_objects WHERE media_type = 'image'"
    connection = None
    db_images = []
    try:
        connection = psycopg2.connect(
            user=os.getenv('DB_USERNAME'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_SERVER'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME')
        )
        cursor = connection.cursor()
        cursor.execute(query)
        db_images = cursor.fetchall()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while connecting to PostgreSQL: {error}")
    finally:
        if connection is not None:
            connection.close()
    return db_images

def get_directory_images(directory):
    return list(Path(directory).glob('**/*'))

def compare_images(db_images, directory_images):
    db_set = {Path(path) / name for name, path in db_images if name and path}
    directory_set = {file for file in directory_images}

    matches = db_set & directory_set
    db_only = db_set - directory_set
    dir_only = directory_set - db_set

    return matches, db_only, dir_only

def generate_csv_report(matches, db_only, dir_only):
    with open('image_comparison_report.csv', 'w', newline='') as csvfile:
        fieldnames = ['Database', 'Directory']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for match in matches:
            writer.writerow({'Database': match, 'Directory': match})
        for db in db_only:
            writer.writerow({'Database': db, 'Directory': ''})
        for dir_ in dir_only:
            writer.writerow({'Database': '', 'Directory': dir_})

if __name__ == "__main__":
    db_images = get_db_images()
    directory_images = get_directory_images(IMAGE_DIRECTORY)
    matches, db_only, dir_only = compare_images(db_images, directory_images)
    generate_csv_report(matches, db_only, dir_only)
    print("Comparison report generated: image_comparison_report.csv")
