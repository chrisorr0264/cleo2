import os
import time
from pathlib import Path
from facelabeler import FaceLabeler
from settings import *
from utilities import Utilities
from dbconnection import get_connection, return_connection, close_pool
from logger_config import setup_logging, get_logger

class FileProcessor:
    def __init__(self, file):
        setup_logging()
        self.logger = get_logger('main')

        self.initialize_variables(file)

        if self.file_type_to_process == 'movie':
            self.process_movie()
        elif self.file_type_to_process == 'image':
            self.process_image()
        else:
            self.logger.error(f"Unknown file type: {self.file_type_to_process}", extra={'class_name': self.__class__.__name__, 'function_name': 'init'})
            raise ValueError(f"Unknown file type: {self.file_type_to_process}")

    def initialize_variables(self, file):
        self.file_to_process, self.file_type_to_process = file
        self.image_folder = IMAGE_DIRECTORY
        self.movies_folder = MOVIES_DIRECTORY
        self.duplicates_folder = DUPLICATE_DIRECTORY
        self.mse_threshold = MSE_THRESHOLD
        self.original_file_name = None
        self.original_file_extension = None
        self.original_file_type = None
        self.file_create_date = None
        self.latitude = None
        self.Longitude = None
        self.location_class = None
        self.location_type = None
        self.location_name = None
        self.location_display_name = None
        self.location_city = None
        self.location_province = None
        self.location_country = None
        self.media_object_id = None
        self.new_file_name = None
        
        self.util = Utilities()
        self.face_labeler = FaceLabeler()

    def process_image(self):
        function_name = 'process_image'
        start_time = time.time()

        self.logger.info(f"Processing file {self.file_to_process}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        # Step 1: Generate tensors
        step_start_time = time.time()
        result = self.util.generate_tensor(self.file_to_process)
        self.logger.detail(f"Step 1: Generate tensor took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        
        if isinstance(result, tuple):
            
            file, tensor_pil, hash_pil, tensor_cv2, hash_cv2 = result

            self.original_file_name = os.path.basename(self.file_to_process)  # Store the original filename
            self.original_file_extension = Path(self.file_to_process).suffix[1:] # Store the original file extension
            self.original_file_type = self.file_type_to_process # Store the original file type
            
            # Step 2: Fetch potential duplicates using PIL and cv2 hashes
            step_start_time = time.time()
            potential_duplicates = self.util.fetch_potential_duplicates(hash_pil, hash_cv2)
            self.logger.detail(f"Step 2: Fetch potential duplicates took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

            # Step 3: Compare tensor with potential duplicates
            step_start_time = time.time()
            duplicates = self.util.compare_with_potential_duplicates(tensor_pil, tensor_cv2, potential_duplicates, self.mse_threshold)
            self.logger.detail(f"Step 3: Compare tensor with potential duplicates took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

            # Step 4: If duplicate, rename and move to duplicate folder
            if duplicates:
                self.handle_duplicate(file, duplicates)
            else:
                self.logger.debug(f"No duplicate found for {file}...processing...", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

                # Step 5: Process the non-duplicate image
                self.process_non_duplicate_image(file, tensor_pil, hash_pil, tensor_cv2, hash_cv2)

        else:
            self.logger.error(f"Failed to generate tensor for {self.file_to_process}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        self.logger.detail(f"Total duration of process_image: {time.time() - start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

    def handle_duplicate(self, file, duplicates):
        function_name = 'handle_duplicate'
        self.logger.debug(f"Duplicates found for {file}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        original_filename = Path(file).stem
        print(f"Original Filename: {original_filename}")

        # Normalize the duplicate path to use Unix-style separators
        duplicate_path = duplicates[0][0].replace('\\','/')
        duplicate_of = Path(duplicate_path).stem
        
        print(f"Duplicate filename (stem): {duplicate_of}")

        mse = duplicates[0][1]
        print(f"MSE: {mse}")

        if Path(file).suffix.lower() in ['.mp4', '.mov', '.avi', '.mkv']:
            fn = f"{original_filename}-DUP_OF_{duplicate_of}{Path(file).suffix}"
        else:
            fn = f"{original_filename}-DUP_OF_{duplicate_of} (mse-{mse}){Path(file).suffix}"


        self.logger.debug(f"File: {fn} is a duplicate and is moved to the duplicates folder.", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        updated_file = os.path.join(self.duplicates_folder, fn)
        step_start_time = time.time()
        self.util.move_file(file, updated_file)
        self.logger.detail(f"Move file to duplicates folder took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

    def process_non_duplicate_image(self, file, tensor_pil, hash_pil, tensor_cv2, hash_cv2):
        function_name = 'process_non_duplicate_image'
        
        # Generate the metadata
        step_start_time = time.time()
        metadata = self.util.get_image_metadata_from_file(file)
        self.logger.detail(f"Generate metadata took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        # Get the original file details
        step_start_time = time.time()
        self.file_create_date = self.util.get_file_create_date_for_image(file, metadata)
        self.logger.detail(f"Get file create date took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        # Get location data details from metadata
        step_start_time = time.time()
        (
            self.latitude,
            self.longitude, 
            self.location_class, 
            self.location_type, 
            self.location_name, 
            self.location_display_name, 
            self.location_city, 
            self.location_province, 
            self.location_country
        ) = self.util.get_file_location_from_metadata(
            metadata, 
            user_agent="image_locator"
        )
        self.logger.detail(f"Get location data details took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        # Create initial tbl_media_object entry
        step_start_time = time.time()
        self.media_object_id = self.util.file_insert(self.original_file_name, self.original_file_type)
        
        self.logger.debug(f"Initial insert of {self.original_file_name} into the database.", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        self.logger.detail(f"Create initial tbl_media_object entry took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        # Calculate the new name
        step_start_time = time.time()
        file_extension = os.path.splitext(file)[1]
        self.new_file_name = self.util.get_new_file_name(self.file_create_date, self.media_object_id)
        self.logger.detail(f"Calculate the new name took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        self.new_file_name += file_extension
        self.new_file_name = os.path.basename(self.new_file_name)  # Use the filename after conversion (if converted)
        
        # Update the database
        step_start_time = time.time()
        self.util.file_update(
            self.new_file_name,
            self.image_folder, 
            self.file_create_date, 
            self.latitude, 
            self.longitude, 
            self.location_class, 
            self.location_type, 
            self.location_name, 
            self.location_display_name, 
            self.location_city, 
            self.location_province, 
            self.location_country, 
            self.media_object_id
        )
        self.logger.detail(f"Update the database took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        flattened_metadata = self.util.flatten_dict(metadata)
        self.util.insert_metadata(flattened_metadata, self.media_object_id)

        # Move the file to the image directory
        step_start_time = time.time()
        updated_file = os.path.join(self.image_folder, self.new_file_name)
        move_file_result = self.util.move_file(file, updated_file)
        self.logger.detail(f"Move file to image directory took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        if move_file_result != 'Success':
            self.logger.error(f"Failed to move file {self.new_file_name} to {updated_file}: {move_file_result}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        # Look for names in the image and update the known_names, invalid_name, tags, and other name tables
        step_start_time = time.time()
        identified_names = self.face_labeler.label_faces_in_image(updated_file, self.media_object_id)
        for name in identified_names:
            self.logger.detail(f'The name: {name} was found in the image: {updated_file}', extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        self.logger.detail(f"Look for names in the image and update tables took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        # Insert the tensor into the tensor table
        step_start_time = time.time()
        self.util.insert_image_tensor(updated_file, tensor_pil, hash_pil, tensor_cv2, hash_cv2, self.media_object_id)
        self.logger.detail(f"Insert the tensor into the tensor table took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

    def process_movie(self):
        function_name = 'process_movie'
        start_time = time.time()

        self.logger.info(f"Processing movie file {self.file_to_process}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        # Step 1: Generate hash
        step_start_time = time.time()
        results = self.util.generate_movie_hash(self.file_to_process)
        self.logger.detail(f"Step 1: Generate movie hash took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        if isinstance(results, tuple):

            file, movie_hash = results

            self.original_file_name = os.path.basename(self.file_to_process)  # Store the original filename
            self.original_file_extension = Path(self.file_to_process).suffix[1:] # Store the original file extension
            self.original_file_type = self.file_type_to_process # Store the original file type                  

            # Step 2: Fetch potential duplicates using PIL and cv2 hashes
            step_start_time = time.time()
            movie_duplicates = self.util.fetch_potential_movie_duplicates(movie_hash)
            self.logger.detail(f"Step 2: Fetch potential duplicates took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

            # Step 3: If duplicate, rename and move to duplicate folder
            if movie_duplicates:
                self.handle_duplicate(file, movie_duplicates)
            else:
                self.logger.debug(f"No duplicate found for {file}...processing...", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

                # Step 4: Process the non-duplicate image
                self.process_non_duplicate_movie(file, movie_hash)

        else:
            self.logger.error(f"Failed to generate movie hash for {self.file_to_process}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        self.logger.detail(f"Total duration of process_image: {time.time() - start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

    def process_non_duplicate_movie(self, file, movie_hash):
        function_name = 'process_non_duplicate_movie'

        # Generate metadata
        step_start_time = time.time()
        metadata = self.util.get_movie_metadata_from_file(file)
        self.logger.detail(f"Step 1: Generate metadata took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        # Get the original file details
        step_start_time = time.time()
        self.file_create_date = self.util.get_file_create_date_for_movie(file, metadata)
        self.logger.detail(f"Step 2: Get file create date took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        #  Get location data details from metadata
        step_start_time = time.time()
        (
            self.latitude,
            self.longitude, 
            self.location_class, 
            self.location_type, 
            self.location_name, 
            self.location_display_name, 
            self.location_city, 
            self.location_province, 
            self.location_country
        ) = self.util.get_file_location_from_movie_metadata(metadata)
        self.logger.detail(f"Step 3: Get location data details took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        # Create initial tbl_media_object entry
        step_start_time = time.time()
        self.media_object_id = self.util.file_insert(self.original_file_name, self.file_type_to_process)
        
        self.logger.debug(f"Initial insert of {self.original_file_name} into the database.", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        self.logger.detail(f"Step 4: Create initial tbl_media_object entry took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        # Calculate the new name
        step_start_time = time.time()
        file_extension = os.path.splitext(file)[1]
        self.new_file_name = self.util.get_new_file_name(self.file_create_date, self.media_object_id)
        self.logger.detail(f"Step 5: Calculate the new name took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        self.new_file_name += file_extension
        self.new_file_name = os.path.basename(self.new_file_name)  # Use the filename after conversion (if converted)
        
        # Update the database
        step_start_time = time.time()
        self.util.file_update(
            self.new_file_name,
            self.movies_folder, 
            self.file_create_date, 
            self.latitude, 
            self.longitude, 
            self.location_class, 
            self.location_type, 
            self.location_name, 
            self.location_display_name, 
            self.location_city, 
            self.location_province, 
            self.location_country, 
            self.media_object_id
        )
        self.logger.detail(f"Step 6: Update the database took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})

        flattened_metadata = self.util.flatten_dict(metadata)
        self.util.insert_metadata(flattened_metadata, self.media_object_id)

        # Move the file to the movies directory
        step_start_time = time.time()
        updated_file = os.path.join(self.movies_folder, self.new_file_name)
        move_file_result = self.util.move_file(self.file_to_process, updated_file)
        self.logger.detail(f"Move file to movies directory took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})
        if move_file_result != 'Success':
            self.logger.error(f"Failed to move file {self.new_file_name} to {updated_file}: {move_file_result}", extra={'class_name': self.__class__.__name__, 'function_name': function_name})


        # Insert the movie hash into the hash table
        step_start_time = time.time()
        self.util.insert_movie_hash(updated_file, movie_hash, self.media_object_id)
        self.logger.detail(f"Insert the movie hash into the hash table took {time.time() - step_start_time:.2f} seconds", extra={'class_name': self.__class__.__name__, 'function_name': function_name})