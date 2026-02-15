import pandas as pd
import os
from dagster import asset

@asset
def dataframe(download_xlsx_file: str):
    """
    Extracts 2012 to 2023 ICE removal data from the local Excel file.
    This acts as our 'Landing' or 'Bronze' layer.
    """
    # download_xlsx_file = "data/raw/ice_removals_2012_2023.xlsx"
    # download_xlsx_file = "data/raw/removals-latest.xlsx"
    
    if not os.path.exists(download_xlsx_file):
        raise Exception(f"File not found at {download_xlsx_file}. Please check your 'data' folder.")

    return pd.read_excel(download_xlsx_file)