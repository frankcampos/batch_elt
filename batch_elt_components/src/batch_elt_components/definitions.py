from dagster import Definitions ,load_from_defs_folder
from pathlib import Path
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource

# load components 
# components =  load_from_defs_folder(project_root=Path(__file__).parent.parent)
# database = Path('duckdb/data.duckdb')

# dbt_resource = DbtCliResource(
#     project_dir=dbt_project,
# )
database_path = Path(__file__).parent.parent.parent.parent/'duckdb'/'data.duckdb'
defs = Definitions.merge(
    load_from_defs_folder(project_root=Path(__file__).parent.parent)
    
,
    Definitions(resources={
        "database": DuckDBResource(database=str(database_path)),
        # "dbt": dbt_resource,
    },),
)