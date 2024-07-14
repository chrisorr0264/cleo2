'''
A logging utility for the cleo2 project.
2024 Christopher Orr
'''

import logging
from datetime import datetime
import os
from settings import *

def get_logger(name):
    return logging.getLogger('main').getChild(name)

# Define the new log level DETAIL
DETAIL_LEVEL_NUM = 5
logging.addLevelName(DETAIL_LEVEL_NUM, "DETAIL")

def detail(self, message, *args, **kws):
    if self.isEnabledFor(DETAIL_LEVEL_NUM):
        self._log(DETAIL_LEVEL_NUM, message, args, **kws)

logging.Logger.detail = detail

# ANSI escape codes for colored output in console
class AnsiColorFormatter(logging.Formatter):
    COLOR_CODES = {
        'DETAIL': DETAIL_COLOUR,     # Light Blue
        'DEBUG': DEBUG_COLOUR,       # White
        'INFO': INFO_COLOUR,         # Green
        'WARNING': WARNING_COLOUR,   # Yellow
        'ERROR': ERROR_COLOUR,       # Red
        'CRITICAL': CRITICAL_COLOUR  # Magenta
    }
    RESET_CODE = RESET_CODE_COLOUR

    def format(self, record):
        color = self.COLOR_CODES.get(record.levelname, self.RESET_CODE)
        message = super().format(record)
        return f"{color}{message}{self.RESET_CODE}"

class ClassNameFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'class_name'):
            record.class_name = 'unknown'
        if not hasattr(record, 'function_name'):
            record.function_name = 'unknown'
        return super().format(record)

def setup_logging(text_widget=None):
    # Create logs directory if it does not exist
    log_dir = LOG_DIRECTORY
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Generate a unique log file name with date and time
    log_filename = datetime.now().strftime("cleoPy_%Y%m%d_%H%M%S.log")
    log_filepath = os.path.join(log_dir, log_filename)

    # Define formatters
    file_formatter = ClassNameFormatter('%(asctime)s:%(levelname)s:%(name)s:%(class_name)s:%(function_name)s:%(message)s')
    console_formatter = AnsiColorFormatter('%(asctime)s:%(levelname)s:%(name)s:%(class_name)s:%(function_name)s:%(message)s')
        
    # Set up file handler
    file_handler = logging.FileHandler(log_filepath, mode='w')
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.getLevelName(FILE_DEBUG_LEVEL))  # Log messages to the file based on the FILE_DEBUG_LEVEL parameter

    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.getLevelName(CONSOLE_DEBUG_LEVEL))  # Log messages to the console based on the  CONSOLE_DEBUG_LEVEL parameter

    logger = logging.getLogger('main')
    logger.setLevel(DETAIL_LEVEL_NUM)  # Use the numeric value of DETAIL level

    # Remove existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # Add new handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger