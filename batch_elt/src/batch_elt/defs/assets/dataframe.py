import pandas as pd
import os
from dagster import asset, AssetIn

def create_dataframe_asset(spec):
    # maybe I need to add the tabs names in the spec file?
    source_name = spec['source']['file_name']
    name = source_name.split('.')[0]
    
    download_asset_name = f"download_{name}"
    dataframe_asset_name = f"dataframe_{name}"

    @asset(
        name=dataframe_asset_name,
        group_name="bronze",
        ins={"input_xlsx_path": AssetIn(key=download_asset_name)}
    )
    def _dataframe_asset(input_xlsx_path: str):
        if not os.path.exists(input_xlsx_path):
            raise Exception(f"File not found at {input_xlsx_path}.")
        # return pd.read_excel(input_xlsx_path)
        return pd.read_excel(input_xlsx_path, sheet_name = None)

    return _dataframe_asset