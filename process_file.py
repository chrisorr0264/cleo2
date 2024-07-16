import os
from file_processor import FileProcessor

def main():
    # Get the file path from the environment variable
    file_path = os.getenv('NEW_FILE')
    if not file_path:
        print("No file path provided in the environment variable 'NEW_FILE'. Exiting.")
        return
    
    print(f"Starting processing for file {file_path}")
    
    # Create an instance of FileProcessor with the file path
    processor = FileProcessor(file_path)
    
    # Call the method to start processing the file
    processor.process()

if __name__ == "__main__":
    main()
