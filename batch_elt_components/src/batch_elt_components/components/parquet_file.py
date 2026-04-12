import dagster as dg
import pandas as pd
from dagster import asset, AssetIn, Output
from pathlib import Path
from typing import Dict, List

class ParquetFile(dg.Component, dg.Model, dg.Resolvable):
    """COMPONENT SUMMARY HERE.

    COMPONENT DESCRIPTION HERE.
    """
    file_path: str
    file_name: str
    # added fields here will define params when instantiated in Python, and yaml schema via Resolvable

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Add definition construction logic here.
        # Create the asset function
        name = self.file_name.split(".")[0].replace("-","_")
        @asset(
            name= f'parquetfile_{name}',
            group_name="bronze_components",
            key_prefix=["bronze_components"],
            ins = {"DataFrames": AssetIn(key=dg.AssetKey(["bronze_components", f'dataframe_{name}']))}
        )
        # Get the asset data frame 
        def _asset(context: dg.AssetExecutionContext,DataFrames:Dict[str,pd.DataFrame]) -> Output[List[str]]:
                
            # I need the path to store the parquet files
            output_dir = Path(self.file_path) / name
            output_dir.mkdir(parents=True, exist_ok=True)
            # log how many dataframes I have 
            context.log.info(f"Processing {len(DataFrames)} sheets.")
            output_paths = []
            for sheet, df in DataFrames.items():
                context.log.info(f"Processing sheet: {sheet}")
                output_path = output_dir/ f"{sheet.replace(" ","_")}.parquet"
                df.to_parquet(output_path, engine="pyarrow")
                output_paths.append(str(output_path))
            return Output(
                value=output_paths,
                metadata={"paths": output_paths, "format": "parquet"},
            )
        return dg.Definitions(assets=[_asset])
