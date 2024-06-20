import json
import os
import subprocess
import logging
import zipfile

class KaggleDownloader:

    def __init__(self, gitignore_path=None):
        if gitignore_path is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
            gitignore_path = os.path.join(parent_dir, '.gitignore')
            print(gitignore_path)
        self.gitignore_path = gitignore_path
        self.set_environment()
    
    def read_credentials_from_gitignore(self):
        with open(self.gitignore_path) as f:
            for line in f:
                if line.strip().startswith('{'):
                    kaggle_token_info = json.loads(line.strip())
                    return kaggle_token_info
        raise ValueError("Kaggle credentials not found in .gitignore")

    def set_environment(self):
        # Set Kaggle environment variables
        kaggle_token_info = self.read_credentials_from_gitignore()
        kaggle_dir = os.path.expanduser('~/.kaggle')
        os.makedirs(kaggle_dir, exist_ok=True)
        
        # Create kaggle.json file with the token
        kaggle_json_path = os.path.join(kaggle_dir, 'kaggle.json')
        with open(kaggle_json_path, 'w') as f:
            json.dump(kaggle_token_info, f)
        os.chmod(kaggle_json_path, 0o600)  

        # Ensure the cache directory exists
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

    