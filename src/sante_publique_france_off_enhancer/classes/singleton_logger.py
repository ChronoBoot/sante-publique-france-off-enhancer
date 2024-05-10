import logging
import os
from datetime import datetime


class SingletonLogger:
    _instance = None

    @staticmethod
    def get_instance():
        if SingletonLogger._instance is None:
            SingletonLogger()
        return SingletonLogger._instance.logger

    def __init__(self):
        if SingletonLogger._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            SingletonLogger._instance = self
            self.logger = logging.getLogger('logger')
            self.logger.setLevel(logging.DEBUG)  # Set global level to debug
            if not self.logger.handlers:  # Check if handlers already exist
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                base_dir = os.path.dirname(os.path.abspath(__file__))
                main_log_dir = os.path.join(base_dir, '..', '..', '..', 'logs')
                if not os.path.exists(main_log_dir):
                    os.makedirs(main_log_dir)

                log_dir = os.path.join(base_dir, '..', '..', '..', 'logs', f'{datetime.now().strftime("%Y%m%d")}')
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)

                # File handler with debug level
                file_handler = logging.FileHandler(os.path.join(log_dir, f'logger_{timestamp}.log'), encoding='utf-8')
                file_handler.setLevel(logging.DEBUG)
                file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                file_handler.setFormatter(file_formatter)
                self.logger.addHandler(file_handler)

                # Console handler with a higher level (e.g., INFO)
                console_handler = logging.StreamHandler()
                console_handler.setLevel(logging.INFO)
                console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                console_handler.setFormatter(console_formatter)
                self.logger.addHandler(console_handler)
