import dagster as dg
from dagster import asset, AssetIn
import os  
import pandas as pd

class DataFrame(dg.Component, dg.Model, dg.Resolvable):
    """COMPONENT SUMMARY HERE.

    COMPONENT DESCRIPTION HERE.
    """
    file_name : str 
    # file_path: str


    # added fields here will define params when instantiated in Python, and yaml schema via Resolvable

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Add definition construction logic here.
        raw_name = self.file_name.split(".")[0]
        name = self.file_name.split(".")[0].replace("-","_")
        @asset(
            name = f'dataframe_{name}',
            group_name= "bronze_components",
            ins = {"DownloadFile": AssetIn(key=dg.AssetKey(["bronze_components", f'download_{raw_name}']))}
        )
        def _asset(DownloadFile:str):
            if not os.path.exists(DownloadFile):
                raise Exception(f'File not found at {DownloadFile}')
            return pd.read_excel(DownloadFile, sheet_name=None)
        return dg.Definitions(assets=[_asset])