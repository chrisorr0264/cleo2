'''
A face recognition utility used throughout the cleo2 project.
2024 Christopher Orr
'''

from PIL import UnidentifiedImageError
import face_recognition
import numpy as np
from dbconnection import get_connection, return_connection
from logger_config import get_logger
import time
from utilities import Utilities


class FaceLabeler:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.known_face_encodings = []
        self.known_face_names = []
        self.util = Utilities()
        self._load_known_faces_from_db()
        

    def _load_known_faces_from_db(self):
        function_name = 'load_known_faces_from_db'
        self.logger.info("Loading known faces from database", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name, encoding FROM tbl_known_faces")
            rows = cursor.fetchall()
            cursor.close()
            for name, encoding in rows:
                self.known_face_names.append(name)
                self.known_face_encodings.append(np.frombuffer(encoding, dtype=np.float64))
            self.logger.info("Loaded known faces from database", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        except Exception as e:
            self.logger.error(f"Error loading known faces from database: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        finally:
            return_connection(conn)

    def add_known_faces(self, names_encodings):
        function_name = 'add_known_faces'
        self.logger.info("Adding known faces to database", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.executemany(
                "INSERT INTO tbl_known_faces (name, encoding) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING",
                [(name, encoding.tobytes()) for name, encoding in names_encodings]
            )
            conn.commit()
            cursor.close()
            for name, encoding in names_encodings:
                self.known_face_names.append(name)
                self.logger.debug(f"Adding {name} to tbl_known_faces.", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                self.known_face_encodings.append(encoding)
            self.logger.info("Added known faces to database", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        except Exception as e:
            self.logger.error(f"Error adding known faces to database: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        finally:
            return_connection(conn)

    def label_faces_in_image(self, image_path, media_object_id):
        function_name = 'label_faces_in_image'
        self.media_object_id = media_object_id  # Store media_object_id as an instance variable
        self.logger.info("Labelling faces in the image.", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        try:
            image = face_recognition.load_image_file(image_path)
        except UnidentifiedImageError as e:
            self.logger.error(f"Failed to load image file {image_path}: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error loading image file {image_path}: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return []

        self.logger.debug(f"Getting the face locations for {image_path}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        face_locations = face_recognition.face_locations(image)
        self.logger.debug(f"Found {len(face_locations)} face(s) in {image_path}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        self.logger.debug(f"Getting the face encodings for {image_path}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        face_encodings = face_recognition.face_encodings(image, face_locations)

        margin = 20
        identified_names = []
        names_encodings_to_add = []

        for (top, right, bottom, left), face_encoding in zip(face_locations, face_encodings):
            self.logger.detail(f"Processing face at location: {(top, right, bottom, left)}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            if self.is_invalid_face_location(media_object_id, (top, right, bottom, left)):
                self.logger.debug(f"Skipping invalid face location: {(top, right, bottom, left)}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                continue

            try:
                start_time = time.time()
                adjusted_top = max(0, top - margin)
                adjusted_right = min(image.shape[1], right + margin)
                adjusted_bottom = min(image.shape[0], bottom + margin)
                adjusted_left = max(0, left - margin)
                self.logger.detail(f"Adjusted face location to: {(adjusted_top, adjusted_right, adjusted_bottom, adjusted_left)}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

                name = "Unknown"
                if self.known_face_encodings:
                    self.logger.debug("Comparing faces", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
                    matches = face_recognition.compare_faces(self.known_face_encodings, face_encoding)
                    face_distances = face_recognition.face_distance(self.known_face_encodings, face_encoding)
                    best_match_index = np.argmin(face_distances)
                    if matches[best_match_index]:
                        name = self.known_face_names[best_match_index]
                        self.logger.detail(f"Match found: {name}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

                if name != "Unknown":
                    identified_names.append((top, right, bottom, left, name))
                    self.logger.debug(f"Added tag for {name}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

                end_time = time.time()
                self.logger.detail(f"Finished processing face in {end_time - start_time} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            except Exception as e:
                self.logger.error(f"Error processing face: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        if names_encodings_to_add:
            self.add_known_faces(names_encodings_to_add)

        self.update_identified_faces_in_db(identified_names, media_object_id)
        return identified_names

    def update_identified_faces_in_db(self, identified_faces, media_object_id):
        function_name = 'update_identified_faces_in_db'
        self.logger.info("Updating identified faces in database", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        conn = get_connection()
        try:
            cursor = conn.cursor()
            # Delete existing identified faces for the media object
            cursor.execute("DELETE FROM tbl_identified_faces WHERE media_object_id = %s", (media_object_id,))
            # Delete existing tags for the media object related to identified faces
            cursor.execute("""
                DELETE FROM tbl_tags_to_media 
                WHERE media_object_id = %s 
                AND tag_id IN (
                    SELECT tag_id 
                    FROM tbl_tags 
                    WHERE tag_name IN (SELECT face_name FROM tbl_identified_faces WHERE media_object_id = %s)
                )
            """, (media_object_id, media_object_id))

            for (_, _, _, _, name) in identified_faces:
                if name != "Unknown":
                    # Insert identified faces
                    cursor.execute("""
                        INSERT INTO tbl_identified_faces (media_object_id, face_name)
                        VALUES (%s, %s)
                    """, (media_object_id, name))
                    
                    # Get or create the tag for the identified face
                    cursor.execute("SELECT tag_id FROM tbl_tags WHERE tag_name = %s", (name,))
                    tag_id = cursor.fetchone()
                    if tag_id is None:
                        cursor.execute("""
                            INSERT INTO tbl_tags (tag_name, tag_desc, created_by, created_IP)
                            VALUES (%s, %s, %s, %s)
                        """, (name, name, self.util.get_logged_in_user(), self.util.get_local_ip()))
                        tag_id = cursor.fetchone()[0]
                    else:
                        tag_id = tag_id[0]
                    
                    # Insert the tag into the tags to media table
                    cursor.execute("""
                        INSERT INTO tbl_tags_to_media (media_object_id, tag_id)
                        VALUES (%s, %s)
                        ON CONFLICT (media_object_id, tag_id) DO NOTHING
                    """, (media_object_id, tag_id))

            conn.commit()
            cursor.close()
            self.logger.debug("Updated identified faces in database", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        except Exception as e:
            self.logger.error(f"Error updating identified faces in database: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        finally:
            return_connection(conn)

    def is_invalid_face_location(self, media_object_id, face_location):
        function_name = 'is_invalid_face_location'
        top, right, bottom, left = face_location
        self.logger.debug(f"Checking if face location is invalid: {face_location}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM tbl_invalid_faces WHERE media_object_id = %s AND \"top\" = %s AND \"right\" = %s AND \"bottom\" = %s AND \"left\" = %s",
                (media_object_id, top, right, bottom, left)
            )
            result = cursor.fetchone()
            cursor.close()
            self.logger.debug(f"Face location invalid check result: {result}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return result is not None
        except Exception as e:
            self.logger.error(f"Error checking if face location is invalid: {e}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
            return False
        finally:
            return_connection(conn)
