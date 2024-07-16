import os
from file_processor import FileProcessor

def main():
    # Get the file path from the environment variable
    new_file_env = os.getenv('NEW_FILE')
    if not new_file_env:
        print("No file path provided in the environment variable 'NEW_FILE'. Exiting.")
        return
    
    # Split the environment variable if it is a tuple-like string
    new_file_parts = new_file_env.split(',')
    if len(new_file_parts) != 2:
        print(f"Invalid NEW_FILE format: {new_file_env}. Expected format: '<file_path>,<file_type>'. Exiting.")
        return
    
    file_path = new_file_parts[0].strip()
    file_type = new_file_parts[1].strip()
    print(f"Starting processing for file {file_path} of type {file_type}")
    
    # Create a tuple with the file path and file type
    file_info = (file_path, file_type)
    
    # Create an instance of FileProcessor with the file info
    processor = FileProcessor(file_info)
    
    # Call the method to start processing the file
    processor.process_image()

if __name__ == "__main__":
    main()

