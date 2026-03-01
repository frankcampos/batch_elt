import pandas as pd
from dagster import asset, Output, AssetIn
import dagster as dg
from typing import Dict, List
from pathlib import Path


def create_parquet_file_asset(spec):
    file_path = spec["destination"]["file_path"]
    name = spec["source"]["file_name"].split(".")[0]
    dataframe_asset_name = f"dataframe_{name}"
    parquet_asset_name = f"parquet_{name}"

    @asset(
        name=parquet_asset_name,
        group_name="bronze",
        key_prefix=["parquet_files"],
        ins={"input_dataframes": AssetIn(key=dataframe_asset_name)},
    )
    def _parquet_file_asset(
        context: dg.AssetExecutionContext, input_dataframes: Dict[str, pd.DataFrame]
    ) -> Output[List[str]]:
        output_dir = Path(file_path) / name
        output_dir.mkdir(parents=True, exist_ok=True)
        context.log.info(f"Processing {len(input_dataframes)} sheets.")
        output_paths = []
        for sheet, df in input_dataframes.items():
            context.log.info(f"Processing sheet: {sheet}")
            output_path = output_dir / f"{sheet.replace(' ', '_')}.parquet"
            df.to_parquet(output_path, engine="pyarrow")
            output_paths.append(str(output_path))
        return Output(
            value=output_paths,
            metadata={"paths": output_paths, "format": "parquet"},
        )

    return _parquet_file_asset
