import os
import pandas as pd
import sqlite3
import logging


class DataProcessor:
    def read_csv_file(self, file_path, encodings):
        # Read a CSV file with multiple encoding attempts
        for enc in encodings:
            try:
                df = pd.read_csv(file_path, encoding=enc)
                logging.info(f"Successfully read {file_path} with encoding: {enc}")
                return df
            except Exception as e:
                logging.warning(f"Failed to read {file_path} with encoding {enc}: {e}")
        logging.error(f"Failed to read {file_path} with all attempted encodings.")
        return None

    def process_crop_data(self, df):
        # Filter crop data for the USA from 1990 to 2018 and sum values by year
        return df[(df['LOCATION'] == 'USA') & (df['TIME'].between(1990, 2018))].groupby('TIME', as_index=False)['Value'].sum()

    def process_ghg_data(self, df):
        # Filter greenhouse gas data for the USA from 1990 to 2018
        columns_to_keep = ['Country/Region', 'unit'] + [str(year) for year in range(1990, 2019)]
        return df[df['Country/Region'] == 'United States'][columns_to_keep]

    def save_to_sqlite(self, df, db_path, table_name):
        # Save dataframe to SQLite database
        os.makedirs('data', exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
        logging.info(f"Data saved to SQLite database at: {db_path}")

    def load_sqlite_table(self, db_path, table_name):
        # Load a table from a SQLite database
        with sqlite3.connect(db_path) as conn:
            return pd.read_sql(f"SELECT * FROM {table_name}", conn)

    def merge_data(self, crop_df, ghg_df):
        # Merge crop production data with greenhouse gas emission data
        ghg_df = ghg_df.melt(id_vars=['Country/Region', 'unit'], var_name='TIME', value_name='Emissions')
        ghg_df['TIME'] = ghg_df['TIME'].astype(int)
        return pd.merge(crop_df, ghg_df[['TIME', 'Emissions']], on='TIME').rename(columns={'Emissions': 'MtCO2e'})
