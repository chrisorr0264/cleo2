import os
import time
import docker
import signal
import requests
from utilities import Utilities
from settings import *
from dotenv import load_dotenv
from dbconnection import DBConnection

MAX_CONTAINERS = 13  # Maximum number of containers to run in parallel
PROCESSING_IMAGE = 'cleo-backend:latest'  # Docker image name
DOCKER_TIMEOUT = 120  # Increase the timeout duration

class Controller:
    def __init__(self):
        load_dotenv()  # Load environment variables from .env file
        self.client = docker.from_env(timeout=DOCKER_TIMEOUT)
        self.new_folder = FILES_TO_PROCESS_DIRECTORY
        self.queue = []
        self.active_containers = []
        self.utils = Utilities()
        self.db_conn_instance = DBConnection.get_instance()
        self.running = True
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGTERM, self.handle_exit)

    def handle_exit(self, signum, frame):
        print("Shutting down gracefully...")
        self.running = False

    def update_queue(self):
        new_files, skipped_files = self.utils.get_new_files(self.new_folder)
        print(f"New files found: {len(new_files)} and Skipped files found: {len(skipped_files)}")
        self.queue.extend([(str(file), file_type) for file, file_type in new_files])

    def manage_queue(self):
        while self.running:  # Keep the controller running indefinitely
            self.update_queue()
            while (self.queue or self.active_containers) and self.running:
                self.cleanup_containers()
                if len(self.active_containers) < MAX_CONTAINERS and self.queue:
                    new_file = self.queue.pop(0)
                    self.start_container(new_file)
                time.sleep(1)  # Adjust sleep time as necessary
            time.sleep(5)  # Wait before checking for new files again

    def start_container(self, file_info):
        file_path, file_type = file_info
        try:
            container = self.client.containers.run(
                PROCESSING_IMAGE,
                environment={
                    'NEW_FILE': f"{file_path},{file_type}"
                },
                volumes={
                    '/mnt/MOM': {'bind': '/mnt/MOM', 'mode': 'rw'}
                },
                detach=True,
                nano_cpus=500000000,
                mem_limit="1g"

            )
            self.active_containers.append(container)
            print(f"Started container {container.id} for file {file_path}")
        except Exception as e:
            print(f"Error starting container for file {file_path}: {e}")
            self.utils.move_to_error_directory(file_path)

    def cleanup_containers(self):
        for container in self.active_containers:
            retries = 3
            while retries > 0:
                try:
                    container.reload()
                    if container.status == 'exited':
                        print(f"Container {container.id} finished")
                        self.active_containers.remove(container)
                        container.remove()
                    break  # Break the retry loop if successful
                except requests.exceptions.ReadTimeout:
                    retries -= 1
                    print(f"Timeout error while reloading container {container.id}. Retrying... ({3-retries}/3)")
                except Exception as e:
                    print(f"Error reloading container {container.id}: {e}")
                    self.utils.move_to_error_directory(container.attrs['Config']['Env'][0].split('=')[1].split(',')[0])
                    self.active_containers.remove(container)
                    container.remove()
                    break  # Break the loop on non-timeout errors

if __name__ == "__main__":
    controller = Controller()
    try:
        controller.manage_queue()
    except KeyboardInterrupt:
        print("Controller interrupted and stopped.")
    finally:
        print("Cleaning up...")
        controller.cleanup_containers()
        print("Controller shut down.")

