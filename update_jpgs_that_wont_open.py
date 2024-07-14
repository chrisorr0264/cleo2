import os
from PIL import Image, UnidentifiedImageError
import pillow_heif
from wand.image import Image as WandImage

def check_file_signature(file_path):
    try:
        with open(file_path, 'rb') as file:
            file_header = file.read(8)  # Read the first 8 bytes of the file
        return file_header
    except Exception as e:
        return f'Error checking file signature: {e}'

def open_heic_image(file_path):
    try:
        heif_file = pillow_heif.read_heif(file_path)
        img = Image.frombytes(
            heif_file.mode,
            heif_file.size,
            heif_file.data,
            "raw",
            heif_file.mode,
            heif_file.stride,
        )
        img.verify()
        return True, None
    except Exception as e:
        return False, str(e)

def convert_heic_to_jpg(input_path, output_path):
    try:
        with WandImage(filename=input_path) as img:
            img.format = 'jpeg'
            img.save(filename=output_path)
        return True, None
    except Exception as e:
        return False, str(e)

def test_specific_images(directory, log_file, filenames):
    # Open the log file in write mode
    with open(log_file, 'w') as log:
        for filename in filenames:
            file_path = os.path.join(directory, filename)
            output_path = os.path.splitext(file_path)[0] + '.jpg'
            
            # Check if the file is an image
            if os.path.isfile(file_path):
                try:
                    # Try to open the image using Pillow
                    with Image.open(file_path) as img:
                        img.verify()
                    # If successful, write to the log file
                    file_signature = check_file_signature(file_path)
                    if file_signature.startswith(b'\x00\x00\x00\x18ftyp'):
                        log.write(f"SUCCESS: {filename} can be opened. Note: Detected file signature indicates it may be an HEIC file. First 8 bytes: {file_signature}. Path: '{file_path}'\n")
                        print(f"SUCCESS: {filename} can be opened. Note: Detected file signature indicates it may be an HEIC file. First 8 bytes: {file_signature}. Path: '{file_path}'")
                    else:
                        log.write(f"SUCCESS: {filename} can be opened. First 8 bytes: {file_signature}. Path: '{file_path}'\n")
                        print(f"SUCCESS: {filename} can be opened. First 8 bytes: {file_signature}. Path: '{file_path}'")
                except (UnidentifiedImageError, IOError):
                    # If the image cannot be opened, check the file signature
                    file_signature = check_file_signature(file_path)
                    if file_signature == b'':
                        log.write(f"FAILURE: {filename} cannot be opened. File is empty or corrupted. Path: '{file_path}'\n")
                        print(f"FAILURE: {filename} cannot be opened. File is empty or corrupted. Path: '{file_path}'")
                    elif file_signature.startswith(b'\x00\x00\x00\x18ftyp'):
                        can_open_heic, heic_error = open_heic_image(file_path)
                        if can_open_heic:
                            # Convert HEIC to JPEG
                            success, conversion_error = convert_heic_to_jpg(file_path, output_path)
                            if success:
                                log.write(f"SUCCESS: {filename} converted from HEIC to JPEG. Path: '{output_path}'\n")
                                print(f"SUCCESS: {filename} converted from HEIC to JPEG. Path: '{output_path}'")
                            else:
                                log.write(f"FAILURE: {filename} could not be converted from HEIC to JPEG. Error: {conversion_error}. Path: '{file_path}'\n")
                                print(f"FAILURE: {filename} could not be converted from HEIC to JPEG. Error: {conversion_error}. Path: '{file_path}'")
                        else:
                            log.write(f"FAILURE: {filename} cannot be opened as an HEIC file. Error: {heic_error}. First 8 bytes: {file_signature}. Path: '{file_path}'\n")
                            print(f"FAILURE: {filename} cannot be opened as an HEIC file. Error: {heic_error}. First 8 bytes: {file_signature}. Path: '{file_path}'")
                    else:
                        log.write(f"FAILURE: {filename} cannot be opened. First 8 bytes: {file_signature}. Path: '{file_path}'\n")
                        print(f"FAILURE: {filename} cannot be opened. First 8 bytes: {file_signature}. Path: '{file_path}'")

# Specify the directory and log file path
directory = 'M:\\Images'
log_file = 'image_test_results.txt'
filenames = [
    '2019-09-17-0008740.jpg',
    '2019-09-17-0008750.jpg',
    '2019-09-20-0008910.jpg',
    '2019-09-20-0008913.jpg',
    '2019-09-22-0009053.jpg',
    '2019-10-20-0010768.jpg'
]

# Call the function
test_specific_images(directory, log_file, filenames)
