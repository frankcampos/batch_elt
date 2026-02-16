import pandas as pd
from dagster import asset, Output, AssetIn

def create_parquet_file_asset(spec):
    file_path = spec['destination']['file_path']
    name = spec['source']['file_name'].split('.')[0]
    dataframe_asset_name = f'dataframe_{name}'
    parquet_asset_name = f'parquet_{name}'

    @asset(
        name=  parquet_asset_name,
        ins = {"input_dataframe": AssetIn(key=dataframe_asset_name)}
    )
    def _parquet_file_asset(input_dataframe: pd.DataFrame):
        
        output_path = file_path + name + '.parquet'

        input_dataframe.to_parquet(output_path, engine='pyarrow')
        
        return Output(
            value=output_path, 
            metadata={"path": output_path, "format": "parquet"}
        )
    return _parquet_file_asset
