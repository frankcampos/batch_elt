from dagster import Definitions
from dagster_duckdb import DuckDBResource
from batch_elt.defs.assets.get_batch_elt_lookup_assets import batch_elt_assets

defs = Definitions(
    assets=batch_elt_assets,
    resources={
        "database": DuckDBResource(database="duckdb/data.duckdb"),
    },
)
