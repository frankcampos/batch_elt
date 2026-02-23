import dagster as dg


all_batch_elt_jobs = dg.define_asset_job(
    name= "all_batch_elt_jobs",
    selection=dg.AssetSelection.all()
)