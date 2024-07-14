import os
import time
from pathlib import Path
from utilities import Utilities
from settings import *
from file_processor import FileProcessor

MAX_PARALLEL_PROCESSES = 5  # Maximum number of parallel processes

class Controller:
    def __init__(self):
        self.new_folder = FILES_TO_PROCESS_DIRECTORY
        self.queue = []
        self.active_processes = 0
        self.utils = Utilities()
        self.processed_files = set()  # Track processed files
        self.failed_files = {}  # Track failed files with retry attempts

    def update_queue(self):
        new_files, skipped_files = self.utils.get_new_files(self.new_folder)
        print(f"New files found: {len(new_files[0])}, Skipped files found: {len(skipped_files)}")
        
        for file in new_files[0]:
            if file not in self.processed_files and file not in self.failed_files:
                self.queue.append(file)

    def manage_queue(self):
        self.update_queue()
        while self.queue or self.active_processes > 0:
            self.cleanup_processes()
            if self.active_processes < MAX_PARALLEL_PROCESSES and self.queue:
                new_file = self.queue.pop(0)
                self.start_processing(new_file)
            time.sleep(1)  # Adjust sleep time as necessary

    def start_processing(self, file):
        print(f"Starting processing for file {file}")
        try:
            processor = FileProcessor(file)
            self.processed_files.add(file)
            self.active_processes += 1
        except Exception as e:
            print(f"Error processing file {file}: {e}")
            if file in self.failed_files:
                self.failed_files[file] += 1
            else:
                self.failed_files[file] = 1
            
            if self.failed_files[file] < 3:  # Retry limit
                self.queue.append(file)  # Retry the file
            else:
                print(f"File {file} failed after 3 attempts.")
                self.processed_files.add(file)  # Do not retry anymore

    def cleanup_processes(self):
        # Here, we assume processing is done sequentially for debugging purposes.
        # In a more advanced scenario, you might want to track process completion.
        if self.active_processes > 0:
            self.active_processes -= 1

if __name__ == "__main__":
    controller = Controller()
    controller.manage_queue()
