import os
import time
import docker
from pathlib import Path
from utilities import Utilities
from settings import *

MAX_CONTAINERS = 5  # Maximum number of containers to run in parallel
PROCESSING_IMAGE = 'cleo-backend:latest'  # Docker image name

class Controller:
    def __init__(self):
        self.client = docker.from_env()
        self.new_folder = FILES_TO_PROCESS_DIRECTORY
        self.queue = []
        self.active_containers = []
        self.utils = Utilities()
        self.logger = self.setup_logging()

    def setup_logging(self):
        # Set up logging here
        pass

    def update_queue(self):
        new_files, skipped_files = self.utils.get_new_files(self.new_folder)
        self.logger.info(f"New files found: {len(new_files)} and Skipped files found: {len(skipped_files)}")
        self.queue.extend(new_files[0])

    def manage_queue(self):
        self.update_queue()
        while self.queue or self.active_containers:
            self.cleanup_containers()
            if len(self.active_containers) < MAX_CONTAINERS and self.queue:
                new_file = self.queue.pop(0)
                self.start_container(new_file)
            time.sleep(1)  # Adjust sleep time as necessary

    def start_container(self, file_info):
        file_path, file_type = file_info
        container = self.client.containers.run(
            PROCESSING_IMAGE,
            environment={
                'NEW_FILE': f"{file_path},{file_type}"
            },
            volumes={
                '/mnt/MOM': {'bind': '/mnt/MOM', 'mode': 'rw'}
            },
            detach=True
        )
        self.active_containers.append(container)
        self.logger.info(f"Started container {container.id} for file {file_path}")

    def cleanup_containers(self):
        for container in self.active_containers:
            container.reload()
            if container.status == 'exited':
                self.logger.info(f"Container {container.id} finished")
                self.active_containers.remove(container)
                container.remove()

if __name__ == "__main__":
    controller = Controller()
    controller.manage_queue()
