import requests
import os
from dagster import asset
def download_xlsx_file(spec):
    source_url = spec['source']['url']
    source_name = spec['source']['file_name']
    file_path = spec['destination']['file_path']
    name= source_name.split('.')[0]
    @asset(name=f"download_{name}")
    def _asset():
        """Extracts 2012 to 2023 ICE removal data from the Deportation Data Project."""
        # The 'raw' part of the URL is the secret to getting the actual file
        # url = "https://github.com/deportationdata/ice/raw/main/data/ice-removals-2012-2023.xlsx"
        # url = "https://github.com/deportationdata/ice/raw/main/data/removals-latest.xlsx"
        
        
        response = requests.get(source_url, stream=True)
        if response.status_code == 200:
            # Match the extension to the source file (.xlsx)
            # file_path = "data/raw/ice_removals_2012_2023.xlsx"
            destination = file_path + source_name

            os.makedirs(file_path, exist_ok=True)
            
            with open(destination, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return destination
        else:
            raise Exception(f"Failed to download data: {response.status_code}. Double-check the URL!")
    return _asset 