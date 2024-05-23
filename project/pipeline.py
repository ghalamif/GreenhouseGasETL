import subprocess
import zipfile
import os
import pandas as pd
import sqlite3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def set_environment():
    """Set environment variables for Kaggle API."""
    os.environ['KAGGLE_CONFIG_DIR'] = os.path.expanduser('~/.kaggle')
    os.environ['KAGGLE_DATASETS_CACHE'] = os.path.expanduser('~/.kaggle/cache')
    if not os.path.exists(os.environ['KAGGLE_DATASETS_CACHE']):
        os.makedirs(os.environ['KAGGLE_DATASETS_CACHE'])

def download_datasets(commands):
    """Download datasets using Kaggle API commands."""
    for command in commands:
        subprocess.run(command, shell=True, check=True)
        logging.info(f"Executed command: {command}")

def extract_zip_files(zip_files, extract_folders):
    """Extract downloaded ZIP files."""
    for zip_file, folder in zip(zip_files, extract_folders):
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(folder)
        os.remove(zip_file)
        logging.info(f"Extracted and removed: {zip_file}")

def read_csv_file(file_path, encodings):
    """Read a CSV file with multiple encoding attempts."""
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
    """Process crop production data to filter for the USA from 1990 to 2018 and calculate sum of Value for each year."""
    df_filtered = df[(df['LOCATION'] == 'USA') & (df['TIME'].between(1990, 2018))]
    df_grouped = df_filtered.groupby('TIME', as_index=False)['Value'].sum()
    return df_grouped

def process_ghg_data(df):
    """Process greenhouse gas emission data to filter for the United States from 1990 to 2018."""
    columns_to_keep = ['Country/Region', 'unit'] + [str(year) for year in range(1990, 2019)]
    df_filtered = df[(df['Country/Region'] == 'United States')][columns_to_keep]
    return df_filtered

def save_to_sqlite(df, db_path, table_name):
    """Save dataframe to SQLite database."""
    if not os.path.exists('data'):
        os.makedirs('data')
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    logging.info(f"Data saved to SQLite database at: {db_path}")

def load_sqlite_table(db_path, table_name):
    """Load a table from a SQLite database."""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

def merge_data(crop_df, ghg_df):
    """Merge crop production data with greenhouse gas emission data."""
    # Assuming the columns in ghg_df are ['Country/Region', 'unit', '1990', '1991', ..., '2018']
    ghg_df_melted = ghg_df.melt(id_vars=['Country/Region', 'unit'], var_name='TIME', value_name='Emissions')
    ghg_df_melted['TIME'] = ghg_df_melted['TIME'].astype(int)
    merged_df = pd.merge(crop_df, ghg_df_melted[['TIME', 'Emissions']], on='TIME')
    merged_df.rename(columns={'Emissions': 'MtCO2e'}, inplace=True)
    return merged_df

def main():
    set_environment()
    
    # Define Kaggle API commands
    commands = [
        'kaggle datasets download -d vagifa/worldwide-crop-production',
        'kaggle datasets download -d saurabhshahane/green-house-gas-historical-emission-data'
    ]
    
    # Download datasets
    download_datasets(commands)
    
    # Extract the downloaded ZIP files
    zip_files = ['worldwide-crop-production.zip', 'green-house-gas-historical-emission-data.zip']
    extract_folders = ['worldwide-crop-production', 'green-house-gas-historical-emission-data']
    extract_zip_files(zip_files, extract_folders)
    
    # Load and process crop production data
    crop_csv_file_path = 'worldwide-crop-production/worldwide_crop_consumption.csv'
    crop_df = read_csv_file(crop_csv_file_path, ['latin1', 'ISO-8859-1', 'cp1252'])
    if crop_df is not None:
        crop_df_processed = process_crop_data(crop_df)
        # Save filtered data to SQLite database
        save_to_sqlite(crop_df_processed, 'data/worldwide_crop_consumption.sqlite', 'crop_production')
    
    # Load and process greenhouse gas emission data
    ghg_csv_file_path = 'green-house-gas-historical-emission-data/ghg-emissions.csv'
    ghg_df = read_csv_file(ghg_csv_file_path, ['latin1', 'ISO-8859-1', 'cp1252'])
    if ghg_df is not None:
        ghg_df_processed = process_ghg_data(ghg_df)
        # Save filtered data to SQLite database
        save_to_sqlite(ghg_df_processed, 'data/ghg-emissions.sqlite', 'ghg_emissions')
    
    # Load data from SQLite databases
    crop_df_sqlite = load_sqlite_table('data/worldwide_crop_consumption.sqlite', 'crop_production')
    ghg_df_sqlite = load_sqlite_table('data/ghg-emissions.sqlite', 'ghg_emissions')
    
    # Merge data
    merged_df = merge_data(crop_df_sqlite, ghg_df_sqlite)
    
    # Save merged data to new SQLite database
    save_to_sqlite(merged_df, 'data/merged.sqlite', 'merged_data')

if __name__ == '__main__':
    main()
