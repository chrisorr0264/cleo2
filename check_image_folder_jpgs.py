import os
from PIL import Image, UnidentifiedImageError

def check_jpg_files(directory):
    total_files = 0
    failed_files = 0
    
    for filename in os.listdir(directory):
        total_files += 1
        file_path = os.path.join(directory, filename)
        
        # Only check files that end with .jpg (case-insensitive)
        if filename.lower().endswith('.jpg'):
            try:
                with Image.open(file_path) as img:
                    img.verify()
                print(f"SUCCESS: {filename} can be opened as a JPEG. ({total_files})")
            except (UnidentifiedImageError, IOError) as e:
                failed_files += 1
                print(f"FAILURE: {filename} cannot be opened as a JPEG. Error: {e} ({total_files})")
        else:
            total_files -= 1  # Do not count non-JPG files

    print(f"\nTotal JPEG files checked: {total_files}")
    print(f"Failed to open: {failed_files}")

# Specify the directory to check
directory = 'M:\\Images'

# Call the function
check_jpg_files(directory)
