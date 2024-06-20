import logging
import re
import os
from DataProcessor import DataProcessor
from KaggleDownloader import KaggleDownloader

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    kaggle_downloader = KaggleDownloader()

    output_Path = 'data/'
    dataset_path = 'datasets/'
    

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
            datasets_info.append({"name": name, "zipFileName": zip_file_name, "SQliteFileName": SQlite_file_name.replace('-','_'), "csvFileName": csv_file_name.replace('-','_')})
    # Create the data directory if it doesn't exist
    if not os.path.exists(dataset_path):
        os.makedirs(dataset_path)

    for dataset in datasets_info:
        kaggle_downloader.extract_zip_file(dataset["zipFileName"], dataset_path)


    data_processor = DataProcessor(datasets_info[1]['csvFileName'], datasets_info[0]['csvFileName'])

    try:
        merged_df = data_processor.merge_crop_emmision()
        merged_df.info()
        data_processor.save_to_sqlite(merged_df, 'data/merged.sqlite', 'merged_crop_emission')
    except Exception as e:
        logging.error(f"Error loading or merging data: {e}")

if __name__ == '__main__':
    main()
