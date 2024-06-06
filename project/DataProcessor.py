import os
import pandas as pd
import numpy as np
import sqlite3
import logging


class DataProcessor:
    def __init__(self, emissions_file, crop_file):
        self.emissions_file = emissions_file
        self.crop_file = crop_file
    
    def load_and_process_emissions(self):
        emissions = pd.read_csv(self.emissions_file, sep=',', na_values=['0.0', '', '0,000', '0'])
        processed_emissions_df = emissions[emissions['Sector'] == 'Agriculture']
        processed_emissions_df = processed_emissions_df.drop(columns=['Unit', 'Data source', 'ISO', 'Sector'])
        processed_emissions_df = processed_emissions_df.melt(id_vars=['Country', 'Gas'], var_name='Year', value_name='Emissions')
        processed_emissions_df = processed_emissions_df.pivot(index=['Country', 'Year'], columns='Gas', values='Emissions').reset_index()
        processed_emissions_df['Year'] = pd.to_datetime(processed_emissions_df['Year'], format='%Y')
        return processed_emissions_df

    def load_and_process_crop_data(self):
        crop = pd.read_csv(self.crop_file, sep=';', na_values=['', '0,000', '0.00', '0.0'])
        if 'Unit' not in crop.columns:
            raise ValueError("The 'Unit' column is missing from the CSV file")
        
        processed_crop_df = crop[crop['Unit'] == 'Thousand tonnes']
        processed_crop_df = processed_crop_df.drop(columns=['Unit', 'Unnamed: 44', 'Unnamed: 45', 'Unnamed: 46'])
        processed_crop_df = processed_crop_df.melt(id_vars=['Location', 'Crop'], var_name='Year', value_name='Production')
        processed_crop_df = processed_crop_df.pivot(index=['Location', 'Year'], columns='Crop', values='Production').reset_index()
        processed_crop_df = processed_crop_df[processed_crop_df['Year'] <= "2020"]
        processed_crop_df['Year'] = pd.to_datetime(processed_crop_df['Year'], format='%Y')
        
        for crop in ['Maize', 'Rice', 'Soybean', 'Wheat']:
            if crop in processed_crop_df.columns:
                processed_crop_df[crop] = processed_crop_df[crop].str.replace(',', '').str.replace('\xa0', '').str.replace(' ', '').astype(float)
        
        processed_crop_df.loc[processed_crop_df['Wheat'] == 0.00, 'Wheat'] = np.nan
        processed_crop_df.replace("China (People's Republic of)", "China", inplace=True)
        return processed_crop_df

    def merge_crop_emmision(self):
        try:
            emissions_df = self.load_and_process_emissions()
            crop_df = self.load_and_process_crop_data()
            merged_df = pd.merge(emissions_df, crop_df, how='inner', left_on=['Country', 'Year'], right_on=['Location', 'Year'])
            merged_df = merged_df.drop(columns=['Location'])
            merged_df = merged_df[merged_df['Country'].isin(['World', 'China', 'India', 'United States'])]
            merged_df['Total Crop Production'] = merged_df[['Maize', 'Rice', 'Soybean', 'Wheat']].sum(axis=1)
            merged_df = merged_df.set_index('Year')
            merged_df = merged_df.interpolate(method='time', limit_direction='both')
            merged_df = merged_df.reset_index()
            return merged_df
        except Exception as e:
            logging.error(f"Error merging crop and emissions data: {e}")
            return None


    def save_to_sqlite(self, df, db_path, table_name):
        os.makedirs('data', exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
        logging.info(f"Data saved to SQLite database at: {db_path}")
        