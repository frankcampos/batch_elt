# maybe create get spec file script?
# the asset that will download the fi
from batch_elt.defs.assets.download_xlsx_file import download_xlsx_file
from batch_elt.defs.assets.dataframe import create_dataframe_asset
from batch_elt.defs.assets.parquet_file import create_parquet_file_asset
from batch_elt.defs.assets.hive_partitioned_parquet_file import create_partitioned_removals_asset
from dagster import get_dagster_logger
from batch_elt.utils.get_specs import get_specs

logger = get_dagster_logger()

def get_batch_elt_lookup_assets(specs):
    batch_elt_lookup_assets = []
    partitioned_removals_asset = create_partitioned_removals_asset()
    for spec in specs:
        download_asset = download_xlsx_file(spec)
        dataframe_asset = create_dataframe_asset(spec)
        parquet_asset = create_parquet_file_asset(spec)
        batch_elt_lookup_assets.append(download_asset)
        batch_elt_lookup_assets.append(dataframe_asset)
        batch_elt_lookup_assets.append(parquet_asset)
    return batch_elt_lookup_assets + [partitioned_removals_asset]

batch_elt_assets = get_batch_elt_lookup_assets(get_specs("batch_elt/src/batch_elt/defs/specs"))

