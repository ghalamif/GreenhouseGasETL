import logging
from DataProcessor import DataProcessor
from KaggleDownloader import KaggleDownloader

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Initialize the KaggleDownloader
    kaggle_downloader = KaggleDownloader()
    
    commands = [
        'kaggle datasets download -d vagifa/worldwide-crop-production',
        'kaggle datasets download -d saurabhshahane/green-house-gas-historical-emission-data'
    ]
    
    kaggle_downloader.download_datasets(commands)
    
    zip_files = ['worldwide-crop-production.zip', 'green-house-gas-historical-emission-data.zip']
    extract_folders = ['worldwide-crop-production', 'green-house-gas-historical-emission-data']
    kaggle_downloader.extract_zip_files(zip_files, extract_folders)
    
    # Initialize the DataProcessor
    data_processor = DataProcessor()
    
    crop_df = data_processor.read_csv_file('worldwide-crop-production/worldwide_crop_consumption.csv', ['latin1', 'ISO-8859-1', 'cp1252'])
    if crop_df is not None:
        crop_df = data_processor.process_crop_data(crop_df)
        data_processor.save_to_sqlite(crop_df, 'data/worldwide_crop_consumption.sqlite', 'crop_production')
    
    ghg_df = data_processor.read_csv_file('green-house-gas-historical-emission-data/ghg-emissions.csv', ['latin1', 'ISO-8859-1', 'cp1252'])
    if ghg_df is not None:
        ghg_df = data_processor.process_ghg_data(ghg_df)
        data_processor.save_to_sqlite(ghg_df, 'data/ghg-emissions.sqlite', 'ghg_emissions')
    
    crop_df_sqlite = data_processor.load_sqlite_table('data/worldwide_crop_consumption.sqlite', 'crop_production')
    ghg_df_sqlite = data_processor.load_sqlite_table('data/ghg-emissions.sqlite', 'ghg_emissions')
    
    merged_df = data_processor.merge_data(crop_df_sqlite, ghg_df_sqlite)
    data_processor.save_to_sqlite(merged_df, 'data/merged.sqlite', 'merged_data')

if __name__ == '__main__':
    main()