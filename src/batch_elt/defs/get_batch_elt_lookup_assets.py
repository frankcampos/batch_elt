# maybe create get spec file script?
# the asset that will download the fi
from batch_elt.defs.assets.download_xlsx_file import download_xlsx_file
from batch_elt.defs.assets.dataframe import create_dataframe_asset
from batch_elt.defs.assets.parquet_file import create_parquet_file_asset
from dagster import AssetSelection, define_asset_job, get_dagster_logger
from libs.get_specs import get_specs

logger = get_dagster_logger()

def get_batch_elt_lookup_assets(specs):
    # create a container 
    batch_elt_lookup_assets = []
    for spec in specs:
        download_asset = download_xlsx_file(spec)
        dataframe_asset = create_dataframe_asset(spec)
        parquet_asset = create_parquet_file_asset(spec)
        batch_elt_lookup_assets.append(download_asset)
        batch_elt_lookup_assets.append(dataframe_asset)
        batch_elt_lookup_assets.append(parquet_asset)
    return batch_elt_lookup_assets

batch_elt_assets = get_batch_elt_lookup_assets(get_specs("src/batch_elt/defs/specs"))

all_assets = AssetSelection.all()

# define a job that will materialize the assets
batch_elt_job = define_asset_job(
    name="batch_elt_job",
    selection=all_assets,
)
