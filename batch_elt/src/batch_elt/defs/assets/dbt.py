import dagster as dg 
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from ...project import dbt_project

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        return "batch_elt"

@dbt_assets(
    manifest=dbt_project.manifest_path,dagster_dbt_translator=CustomDagsterDbtTranslator(),
)

def dbt_all(context:dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"],context=context).stream()
                       