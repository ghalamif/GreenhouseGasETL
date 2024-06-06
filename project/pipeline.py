import logging
import re
import os
from DataProcessor import DataProcessor
from KaggleDownloader import KaggleDownloader

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Initialize the KaggleDownloader
    kaggle_downloader = KaggleDownloader()

    output_Path = 'data/'
    dataset_path = 'datasets/'
    #csv_options = ['latin1', 'ISO-8859-1', 'cp1252']

    commands = [
        'kaggle datasets download -d farzanghalami/worldwide-crop-production',
        'kaggle datasets download -d farzanghalami/historical-emissions'
        
    ]
    
    kaggle_downloader.download_datasets(commands)
    
    datasets_info = []
    for command in commands:
        match = re.search(r'\/([^\s]+)', command)
        if match:
            name = match.group(1)
            zip_file_name = f"{name}.zip"
            SQlite_file_name = f"{output_Path}{name}.sqlite"
            csv_file_name = f"{dataset_path}{name}.csv"
            #if name == 'worldwide-crop-production':
                #csv_file_name = f"{dataset_path}worldwide_crop_consumption.csv"
            datasets_info.append({"name": name, "zipFileName": zip_file_name, "SQliteFileName": SQlite_file_name.replace('-','_'), "csvFileName": csv_file_name.replace('-','_')})
    # Create the data directory if it doesn't exist
    if not os.path.exists(dataset_path):
        os.makedirs(dataset_path)

    for dataset in datasets_info:
        kaggle_downloader.extract_zip_file(dataset["zipFileName"], dataset_path)

        # List files in the dataset_path directory to ensure files are extracted
        extracted_files = os.listdir(dataset_path)
        logging.info(f"Files in {dataset_path}: {extracted_files}")

        # Log full paths of files in the dataset_path directory
        for root, dirs, files in os.walk(dataset_path):
            for file in files:
                logging.info(f"Extracted file: {os.path.join(root, file)}")

    # Initialize the DataProcessor
    data_processor = DataProcessor(datasets_info[1]['csvFileName'], datasets_info[0]['csvFileName'])

    try:
        merged_df = data_processor.merge_crop_emmision()
        merged_df.info()
        data_processor.save_to_sqlite(merged_df, 'data/merged.sqlite', 'merged_crop_emission')
    except Exception as e:
        logging.error(f"Error loading or merging data: {e}")

if __name__ == '__main__':
    main()
