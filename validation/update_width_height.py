import os
import logging
from dotenv import load_dotenv
import sys
from media_dimension_extractor import MediaDimensionExtractor
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dbconnection import DBConnection

load_dotenv()

logger = logging.getLogger('update_dimensions')
logging.basicConfig(level=logging.INFO)

def update_dimensions():
    image_root = os.getenv('IMAGE_ROOT', '/mnt/MOM/Images')
    video_root = os.getenv('VIDEO_ROOT', '/mnt/MOM/Movies')
    extractor = MediaDimensionExtractor(image_root, video_root)

    db = DBConnection.get_instance()
    conn = db.get_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT media_object_id, new_name, media_type FROM tbl_media_objects")
        media_files = cursor.fetchall()

        for i, (media_id, new_name, media_type) in enumerate(media_files, start=1):
            print(f"Processing record {i}/{len(media_files)}: {new_name}")

            width, height = extractor.get_dimensions(media_type, new_name)

            if width is not None and height is not None:
                cursor.execute(
                    "UPDATE tbl_media_objects SET width = %s, height = %s WHERE media_object_id = %s",
                    (width, height, media_id)
                )
                print(f"Updated {new_name} with width={width} and height={height}")
            else:
                print(f"Skipped {new_name} due to error in getting dimensions")

        conn.commit()
    except Exception as e:
        logger.error(f"Error updating dimensions: {e}")
    finally:
        db.return_connection(conn)

if __name__ == "__main__":
    update_dimensions()
