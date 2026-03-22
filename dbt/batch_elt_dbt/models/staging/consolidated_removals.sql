{{config(
    materialized = 'table',
    incremental_strategy = 'append',
    on_schema_change= 'sync_all_columns',
    database = 'my_ducklake'
)}}
select DISTINCT *  
from {{source('removals','consolidated_removals')}}