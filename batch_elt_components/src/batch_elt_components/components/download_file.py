import dagster as dg 
from dagster import asset
import requests
import os 

class DownloadFile(dg.Component, dg.Model, dg.Resolvable):
    """COMPONENT SUMMARY HERE.

    COMPONENT DESCRIPTION HERE.
    """
    url : str 
    file_name: str
    destination_path:str

    # added fields here will define params when instantiated in Python, and yaml schema via Resolvable

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Add definition construction logic here.
        name = self.file_name.split('.')[0]
        @dg.asset(
              name = f"download_{name}",
              key_prefix=["bronze_components"],
              group_name="bronze_components"  
        ) 
        def _asset():
            destination = self.destination_path + self.file_name
            if os.path.exists(destination):
                return destination
            response = requests.get(self.url , stream=True)
            if response.status_code == 200:
                # destination = self.destination_path + self.file_name
                os.makedirs(self.destination_path, exist_ok=True)
                with open(destination, 'wb') as f: 
                    for chunck in response.iter_content(chunk_size=8192):
                        f.write(chunck)
                return destination
            else: 
                raise Exception(f'Failed to download data: {response.status_code}. Double-check the URL!')
        return dg.Definitions(assets=[_asset])
