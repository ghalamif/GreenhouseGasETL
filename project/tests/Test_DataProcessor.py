import unittest
import pandas as pd
import os
import sqlite3
import sys
import logging
sys.path.insert(0, '..')  
from DataProcessor import DataProcessor

logging.basicConfig(level=logging.DEBUG)  

class TestDataProcessor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.emissions_file = '../datasets/historical_emissions.csv'
        cls.crop_file = '../datasets/worldwide_crop_production.csv'
        cls.db_path = '../data/merged.sqlite'
        cls.table_name = 'merged_crop_emission'
        
        cls.processor = DataProcessor(cls.emissions_file, cls.crop_file)

    def setUp(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

#########################################################################################################################
############################################ emission ###################################################################
#########################################################################################################################
    def test_load_and_process_emissions(self):
        self.assertTrue(os.path.isfile(self.emissions_file), "emission CSV file was not created.")
        processed_emissions = self.processor.load_and_process_emissions()
        self.assertFalse(processed_emissions.empty, "Processed emissions DataFrame is empty.")

        allowed_columns = ['CH4', 'N2O', 'All GHG', 'Country', 'Year']
        for column in allowed_columns:
            self.assertIn(column, processed_emissions.columns, f"{column} column missing in emissions data frame.")

        #data type
        data_types = {
            'Year': 'datetime64[ns]',
            'CH4': 'float64',
            'N2O': 'float64',
            'All GHG': 'float64',
            'Country': 'object'
        }
        for column, dtype in data_types.items():
            self.assertEqual(processed_emissions[column].dtype, dtype, f"Data type of {column} column is not {dtype}.")

        # check that Year should be between 1990 and 2020
        year_min = processed_emissions['Year'].dt.year.min()
        year_max = processed_emissions['Year'].dt.year.max()
        self.assertGreaterEqual(year_min, 1990, f"Year column contains years before 1990")
        self.assertLessEqual(year_max, 2020, f"Year column contains years after 2020")

#########################################################################################################################
############################################ CROP ######################################################################
#########################################################################################################################
    def test_load_and_process_crop_data(self):
        self.assertTrue(os.path.isfile(self.crop_file), "crop-production CSV file was not created.")
        processed_crop = self.processor.load_and_process_crop_data()
        self.assertFalse(processed_crop.empty, "Processed crop DataFrame is empty.")
        allowedcolmns = ['Maize', 'Year', 'Soybean', 'Wheat', 'Rice', 'Location']

        for column in allowedcolmns:
            self.assertIn(column, processed_crop.columns, f"{column} column missing in crop data frame.")

        # check data types
        data_types = {
            'Year': 'datetime64[ns]',
            'Maize': 'float64',
            'Soybean': 'float64',
            'Wheat': 'float64',
            'Rice': 'float64',
            'Location': 'object'
        }
        for column, dtype in data_types.items():
            self.assertEqual(processed_crop[column].dtype, dtype, f"Data type of {column} column is not {dtype}.")

        # check that Year should be between 1990 and 2020
        year_min = processed_crop['Year'].dt.year.min()
        year_max = processed_crop['Year'].dt.year.max()
        self.assertGreaterEqual(year_min, 1990, f"Year column contains years before 1990")
        self.assertLessEqual(year_max, 2020, f"Year column contains years after 2020")

#########################################################################################################################
############################################ merge_crop_emission ########################################################
#########################################################################################################################       
    def test_merge_crop_emission(self):
        merged_data = self.processor.merge_crop_emmision()
        self.assertIsNotNone(merged_data, "Merged DataFrame is None.")
        self.assertFalse(merged_data.empty, "Merged DataFrame is empty.")
        self.assertIn('Total Crop Production', merged_data.columns, "Total Crop Production column missing in merged DataFrame.")

        columns = ['Year', 'Country','All GHG','CH4','N2O','Maize','Rice','Soybean','Wheat','Total Crop Production']
        for column in columns:
            self.assertIn(column, merged_data, f"{column} column missing in the merged dataframe.")
        
        # check that Year should be between 1990 and 2020
        year_min = merged_data['Year'].dt.year.min()
        year_max = merged_data['Year'].dt.year.max()
        self.assertGreaterEqual(year_min, 1990, f"Year column contains years before 1990")
        self.assertLessEqual(year_max, 2020, f"Year column contains years after 2020")

        allowed_country = ['China', 'India', 'United States','World']
        all_mentioned_country =  merged_data['Country'].unique()
        # Convert lists to sets and compare
        self.assertEqual(set(allowed_country), set(all_mentioned_country), 
                            f"Countries mentioned in data ({all_mentioned_country}) do not match allowed countries ({allowed_country}).")
        
        # check data types
        expected_types = {
            'Year': 'datetime64[ns]',
            'Country': 'object',
            'All GHG': 'float64',
            'CH4': 'float64',
            'N2O': 'float64',
            'Maize': 'float64',
            'Rice': 'float64',
            'Soybean': 'float64',
            'Wheat': 'float64',
            'Total Crop Production': 'float64'
        }
        for column, dtype in expected_types.items():
            self.assertEqual(merged_data[column].dtype, dtype, f"Data type of {column} column is not {dtype}.")


#########################################################################################################################
############################################ save_to_sqlite ##############################################################
######################################################################################################################### 
    def test_save_to_sqlite(self):
        merged_data = self.processor.merge_crop_emmision()
        self.processor.save_to_sqlite(merged_data, self.db_path, self.table_name)
        self.assertTrue(os.path.isfile(self.db_path), "SQLite database file was not created.")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table_name}';")
        table_exists = cursor.fetchone()
        self.assertIsNotNone(table_exists, f"Table '{self.table_name}' does not exist in the database.")
        
        df = pd.read_sql_query(f"SELECT * FROM {self.table_name}", conn)
        self.assertFalse(df.empty, "Data in the SQLite table is empty.")

        columns = ['Year', 'Country','All GHG','CH4','N2O','Maize','Rice','Soybean','Wheat','Total Crop Production']
        for column in columns:
            self.assertIn(column, df.columns, f"{column} column missing in the table.")

        # check YEAR
        year_min = pd.to_datetime(df['Year']).dt.year.min()
        year_max = pd.to_datetime(df['Year']).dt.year.max()
        self.assertGreaterEqual(year_min, 1990, f"Year column contains years before 1990")
        self.assertLessEqual(year_max, 2020, f"Year column contains years after 2020")

    
        expected_types = {  
            'Year': 'TEXT',
            'Country': 'TEXT',
            'All GHG': 'REAL',
            'CH4': 'REAL',
            'N2O': 'REAL',
            'Maize': 'REAL',
            'Rice': 'REAL',
            'Soybean': 'REAL',
            'Wheat': 'REAL',
            'Total Crop Production': 'REAL'
        }
        for column, dtype in expected_types.items():
            self.assertEqual(df[column].dtype, dtype, f"Data type of {column} column is not {dtype}.")
        conn.close()

if __name__ == '__main__':
    unittest.main()
