import os
import sys
from pyicloud import PyiCloudService
from pyicloud.exceptions import PyiCloudFailedLoginException
from tqdm import tqdm
import concurrent.futures
import time

# Your iCloud credentials
apple_id = 'chris@tranquilcs.com'
password = "Let'sbestrong1"

# Directory to save the photos and videos
download_dir = '/mnt/MOM/New'

def authenticate_icloud(apple_id, password):
    try:
        print("Authenticating with iCloud...")
        api = PyiCloudService(apple_id, password)
        if api.requires_2fa:
            print("Two-factor authentication required.")
            code = input("Enter the code you received: ")
            result = api.validate_2fa_code(code)
            if not result:
                print("Failed to verify 2FA code.")
                sys.exit(1)
            if not api.is_trusted_session:
                print("Session is not trusted. Requesting trust...")
                result = api.trust_session()
                if not result:
                    print("Failed to request trust. You may be prompted for 2FA code again.")
        print("Authenticated successfully.")
        return api
    except PyiCloudFailedLoginException as e:
        print(f"Failed to log in to iCloud: {e}")
        sys.exit(1)

def get_unique_filename(directory, filename):
    base, extension = os.path.splitext(filename)
    counter = 1
    new_filename = filename
    while os.path.exists(os.path.join(directory, new_filename)):
        new_filename = f"{base}_{counter}{extension}"
        counter += 1
    return new_filename

def download_media_file(media, download_dir):
    file_name = media.filename
    file_path = os.path.join(download_dir, file_name)
    
    if os.path.exists(file_path):
        file_name = get_unique_filename(download_dir, file_name)
        file_path = os.path.join(download_dir, file_name)

    retries = 3
    for attempt in range(retries):
        try:
            with open(file_path, 'wb') as f:
                print(f"Downloading {file_name}...")
                download = media.download()
                if download:
                    f.write(download.raw.read())
                    return file_name, "Downloaded"
                else:
                    raise Exception("Download object is None")
        except Exception as e:
            print(f"Error downloading {file_name}: {e}, attempt {attempt + 1}/{retries}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            return file_name, f"Error: {e}"

def download_files(api, download_dir):
    all_files = api.photos.all
    total_files = len(all_files)
    if total_files == 0:
        print(f"No media found in iCloud.")
        return

    print(f"Found {total_files} media files in iCloud.")
    os.makedirs(download_dir, exist_ok=True)

    downloaded_files = 0
    skipped_files = 0
    failed_files = 0

    print("Preparing to download media files...")
    with tqdm(total=total_files, desc=f"Downloading media files") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_media = {}
            for media in all_files:
                print(f"Submitting download task for: {media.filename}")
                future = executor.submit(download_media_file, media, download_dir)
                future_to_media[future] = media

            for future in concurrent.futures.as_completed(future_to_media):
                media = future_to_media[future]
                try:
                    file_name, status = future.result()
                    if "Downloaded" in status:
                        downloaded_files += 1
                    else:
                        failed_files += 1
                        print(f"Error downloading {file_name}: {status}")
                except Exception as e:
                    failed_files += 1
                    print(f"Error downloading {media.filename}: {e}")
                pbar.update(1)

    print(f"\nMedia download summary:")
    print(f"Total media files: {total_files}")
    print(f"Downloaded: {downloaded_files}")
    print(f"Skipped: {skipped_files} (already exists)")
    print(f"Failed: {failed_files} (errors)")

if __name__ == "__main__":
    print("Starting iCloud download script...")
    api = authenticate_icloud(apple_id, password)

    # Download all media files (photos and videos)
    print("Starting download of all media files...")
    download_files(api, download_dir)

    print("Download process completed.")