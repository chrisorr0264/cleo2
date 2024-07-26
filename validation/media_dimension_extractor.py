import os
import logging
from PIL import Image
import ffmpeg

logger = logging.getLogger('media_dimension_extractor')
logging.basicConfig(level=logging.INFO)

class MediaDimensionExtractor:
    def __init__(self, image_root, video_root):
        self.image_root = image_root
        self.video_root = video_root

    def get_image_dimensions(self, file_path):
        try:
            with Image.open(file_path) as img:
                width, height = img.size
                return width, height
        except Exception as e:
            logger.error(f"Error processing image {file_path}: {e}")
            return None, None

    def get_video_dimensions(self, file_path):
        try:
            probe = ffmpeg.probe(file_path)
            video_stream = next(stream for stream in probe['streams'] if stream['codec_type'] == 'video')
            width = int(video_stream['width'])
            height = int(video_stream['height'])
            return width, height
        except Exception as e:
            logger.error(f"Error getting video dimensions for {file_path}: {e}")
            return None, None

    def get_dimensions(self, media_type, file_name):
        if media_type == 'image':
            file_path = os.path.join(self.image_root, file_name)
            return self.get_image_dimensions(file_path)
        elif media_type == 'movie':
            file_path = os.path.join(self.video_root, file_name)
            return self.get_video_dimensions(file_path)
        else:
            logger.error(f"Unsupported media type {media_type} for {file_name}")
            return None, None
