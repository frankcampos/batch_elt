from dagster import Definitions, load_from_defs_folder
from dagster_duckdb import DuckDBResource
# add dagster cli
from dagster_dbt import DbtCliResource
# import  dbt_project 
from batch_elt.project import dbt_project
from batch_elt.defs.assets.get_batch_elt_lookup_assets import batch_elt_assets
from batch_elt.defs.assets.dbt import dbt_all
from pathlib import Path 



# dbt cli access to the project
dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)
# dbt_models = load_from_defs_folder(project_root=Path(__file__)/'assets')

defs = Definitions(
    assets=batch_elt_assets + [dbt_all],
    resources={
        "database": DuckDBResource(database="duckdb/data.duckdb"),
        "dbt": dbt_resource,
    },
)
