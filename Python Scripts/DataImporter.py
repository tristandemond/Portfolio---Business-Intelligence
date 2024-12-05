"""
Data Importer class

This class provides methods to import data from various sources like CSV, Excel and Google Sheets.
After importing, the data is stored in a dictionary where the keys are the names of the dataframes
and the values are the dataframes themselves.

The class also provides methods to list all imported dataframes and to retrieve a specific dataframe.

The data can be imported from a folder containing CSV and Excel files. The files are read and stored
in a dataframe with the same name as the file name without extension. For example, a file named
"example.csv" will be stored in a dataframe named "example".

The data can also be imported from Google Sheets based on a configuration file. The configuration file
should contain one line per Google Sheet with the URL of the sheet and the name of the dataframe to store
the data in. For example:

https://docs.google.com/spreadsheets/d/1234567890,example

The data will be stored in a dataframe named "example".

The data can also be imported from a Google Sheets configuration file where the file name is specified
in the gsheet_config.txt file in the same folder as the script.

The data can be pushed to a SQLite database. The data is upserted into the database.

Example usage:

importer = DataImporter()
importer.import_source_data('folder_with_data')
dataframes = importer.list_dataframes()
df = importer.get_dataframe('example')
"""
import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Class to handle data import and structuring
class DataImporter:
    def __init__(self):
        self.dataframes = {}

    def read_csv(self, file_path, df_name):
        """Read a CSV file and store it in a dataframe."""
        try:
            df = pd.read_csv(file_path)
            self.dataframes[df_name] = df
            print(type(df))
            print(f"CSV file {file_path} imported as {df_name} dataframe.")
        except Exception as e:
            print(f"Error importing CSV file: {e}")

    def read_excel(self, file_path, df_name):
        """Read an Excel file and store it in a dataframe."""
        try:
            df = pd.read_excel(file_path)
            self.dataframes[df_name] = df
            print(f"Excel file {file_path} imported as {df_name} dataframe.")
        except Exception as e:
            print(f"Error importing Excel file: {e}")

    def read_google_sheet(self, sheet_url, df_name, credentials_file):
        """Read a Google Sheet and store it in a dataframe."""
        try:
            # Define scope for accessing Google Sheets
            scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
            client = gspread.authorize(creds)

            # Extract the Google Sheets ID from the URL
            sheet_id = sheet_url.split("/d/")[1].split("/")[0]
            sheet = client.open_by_key(sheet_id).sheet1

            # Get all records and convert to a DataFrame
            data = sheet.get_all_records()
            df = pd.DataFrame(data)
            self.dataframes[df_name] = df
            print(f"Google Sheet {sheet_url} imported as {df_name} dataframe.")
        except Exception as e:
            print(f"Error importing Google Sheet: {e}")

    def import_source_data(self, folder_path):
        """Import all CSV and Excel files in the given folder as separate dataframes."""
        try:
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)
                if file_name.endswith('.csv'):
                    # Use file name without extension as dataframe name
                    df_name = os.path.splitext(file_name)[0]
                    self.read_csv(file_path, df_name)
                elif file_name.endswith(('.xls', '.xlsx')):
                    df_name = os.path.splitext(file_name)[0]
                    self.read_excel(file_path, df_name)
                elif file_name == "gsheet_config.txt":
                    # If the file is the gsheet config, import Google Sheets from it
                    credentials_fname = os.path.join(".","data_ingestion","Script","credentials.json")
                    self.import_google_sheet_from_config(file_path, credentials_fname)  # Assuming the credentials.json is in the same directory
                else:
                    print(f"Unsupported file type: {file_name}. Skipping.")
        except Exception as e:
            print(f"Error importing files from folder: {e}")

    def import_google_sheet_from_config(self, config_file, credentials_file):
        """Import Google Sheets based on configuration file."""
        try:
            with open(config_file, "r") as f:
                for line in f:
                    sheet_url, df_name = line.strip().split(",")
                    self.read_google_sheet(sheet_url, df_name, credentials_file)
        except Exception as e:
            print(f"Error reading config file '{{config_file}}': {e}")

    def get_dataframe(self, df_name):
        """Retrieve a stored dataframe."""
        return self.dataframes.get(df_name, None)

    def list_dataframes(self):
        """List all imported dataframes."""
        return list(self.dataframes.keys())


