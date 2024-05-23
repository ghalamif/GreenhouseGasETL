import subprocess
import zipfile
import os
import pandas as pd
import sqlite3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def set_environment():
#Set environment variables for Kaggle API.
    kaggle_dir = os.path.expanduser('~/.kaggle')
    cache_dir = os.path.join(kaggle_dir, 'cache')
    os.environ['KAGGLE_CONFIG_DIR'] = kaggle_dir
    os.environ['KAGGLE_DATASETS_CACHE'] = cache_dir
    os.makedirs(cache_dir, exist_ok=True)

def download_datasets(commands):
#Download datasets using Kaggle API commands.
    for command in commands:
        subprocess.run(command, shell=True, check=True)
        logging.info(f"Executed command: {command}")

def extract_zip_files(zip_files, extract_folders):
#Extract downloaded ZIP files.
    for zip_file, folder in zip(zip_files, extract_folders):
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(folder)
        os.remove(zip_file)
        logging.info(f"Extracted and removed: {zip_file}")

def read_csv_file(file_path, encodings):
#Read a CSV file with multiple encoding attempts
    for enc in encodings:
        try:
            df = pd.read_csv(file_path, encoding=enc)
            logging.info(f"Successfully read {file_path} with encoding: {enc}")
            return df
        except Exception as e:
            logging.warning(f"Failed to read {file_path} with encoding {enc}: {e}")
    logging.error(f"Failed to read {file_path} with all attempted encodings.")
    return None

def process_crop_data(df):
#Filter crop data for the USA from 1990 to 2018 and sum values by year
    return df[(df['LOCATION'] == 'USA') & (df['TIME'].between(1990, 2018))].groupby('TIME', as_index=False)['Value'].sum()

def process_ghg_data(df):
#Filter greenhouse gas data for the USA from 1990 to 2018
    columns_to_keep = ['Country/Region', 'unit'] + [str(year) for year in range(1990, 2019)]
    return df[df['Country/Region'] == 'United States'][columns_to_keep]

def save_to_sqlite(df, db_path, table_name):
#Save dataframe to SQLite database
    os.makedirs('data', exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
    logging.info(f"Data saved to SQLite database at: {db_path}")

def load_sqlite_table(db_path, table_name):
    #Load a table from a SQLite database
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)

def merge_data(crop_df, ghg_df):
    #Merge crop production data with greenhouse gas emission data
    ghg_df = ghg_df.melt(id_vars=['Country/Region', 'unit'], var_name='TIME', value_name='Emissions')
    ghg_df['TIME'] = ghg_df['TIME'].astype(int)
    return pd.merge(crop_df, ghg_df[['TIME', 'Emissions']], on='TIME').rename(columns={'Emissions': 'MtCO2e'})

def main():
    set_environment()
    
    commands = [
        'kaggle datasets download -d vagifa/worldwide-crop-production',
        'kaggle datasets download -d saurabhshahane/green-house-gas-historical-emission-data'
    ]
    
    download_datasets(commands)
    
    zip_files = ['worldwide-crop-production.zip', 'green-house-gas-historical-emission-data.zip']
    extract_folders = ['worldwide-crop-production', 'green-house-gas-historical-emission-data']
    extract_zip_files(zip_files, extract_folders)
    
    crop_df = read_csv_file('worldwide-crop-production/worldwide_crop_consumption.csv', ['latin1', 'ISO-8859-1', 'cp1252'])
    if crop_df is not None:
        crop_df = process_crop_data(crop_df)
        save_to_sqlite(crop_df, 'data/worldwide_crop_consumption.sqlite', 'crop_production')
    
    ghg_df = read_csv_file('green-house-gas-historical-emission-data/ghg-emissions.csv', ['latin1', 'ISO-8859-1', 'cp1252'])
    if ghg_df is not None:
        ghg_df = process_ghg_data(ghg_df)
        save_to_sqlite(ghg_df, 'data/ghg-emissions.sqlite', 'ghg_emissions')
    
    crop_df_sqlite = load_sqlite_table('data/worldwide_crop_consumption.sqlite', 'crop_production')
    ghg_df_sqlite = load_sqlite_table('data/ghg-emissions.sqlite', 'ghg_emissions')
    
    merged_df = merge_data(crop_df_sqlite, ghg_df_sqlite)
    save_to_sqlite(merged_df, 'data/merged.sqlite', 'merged_data')

if __name__ == '__main__':
    main()

