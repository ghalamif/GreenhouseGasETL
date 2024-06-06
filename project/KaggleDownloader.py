import os
import subprocess
import logging
import zipfile

class KaggleDownloader:
    def __init__(self):
        self.set_environment()
    
    def set_environment(self):
        # Set environment variables for Kaggle API.
        kaggle_dir = os.path.expanduser('~/.kaggle')
        cache_dir = os.path.join(kaggle_dir, 'cache')
        os.environ['KAGGLE_CONFIG_DIR'] = kaggle_dir
        os.environ['KAGGLE_DATASETS_CACHE'] = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def download_datasets(self, commands):
        # Download datasets using Kaggle API commands.
        for command in commands:
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Executed command: {command}")

    def extract_zip_files(self, zip_files, extract_folders):
        # Extract downloaded ZIP files.
        for zip_file, folder in zip(zip_files, extract_folders):
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(folder)
            os.remove(zip_file)
            logging.info(f"Extracted and removed: {zip_file}")

    def extract_zip_file(self, zip_file, extract_folder):
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        os.remove(zip_file)
        logging.info(f"Extracted and removed: {zip_file}")