import pandas as pd
import dagster as dg
# import parquet write
from fastparquet import write



@dg.asset
def parquet_file(dataframe: pd.DataFrame):
    
    # output_path = 'data/raw/ice_removals_2012_2023.parquet'
    output_path = 'data/raw/removals-latest.parquet'

    dataframe.to_parquet(output_path, engine='fastparquet')
    
    return dg.Output(
        value=output_path, 
        metadata={"path": output_path, "format": "parquet"}
    )

