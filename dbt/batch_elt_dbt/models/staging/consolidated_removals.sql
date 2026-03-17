{{config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change= 'sync_all_columns',
    database = 'my_ducklake'
)}}
select *  
from {{source('removals','consolidated_removals')}}