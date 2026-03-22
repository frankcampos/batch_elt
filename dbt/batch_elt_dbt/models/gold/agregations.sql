{{config(
    materialized = 'table',
    database = 'my_ducklake'
)}}

select
    anonymized_identifier,

    -- static personal attributes
    gender,
    birth_year,
    birth_country,
    citizenship_country,
    criminal_charge,
    criminal_charge_status,

    -- aggregated event info
    count(*)                                      as total_removal_events,
    min(departure_date)                           as earliest_removal_date,
    max(departure_date)                           as most_recent_removal_date,
    count(distinct year)                          as total_years_with_removals,
    count(distinct departure_port)                as total_distinct_departure_ports,
    count(distinct enforcement_program)           as total_distinct_enforcement_programs,
    extract(year from current_date) - birth_year  as estimated_current_age,

    -- breakdown by type
    count(case when disposition = 'Voluntary Return'
          then 1 end)                             as total_voluntary_returns,
    count(case when disposition like 'REINSTATEMENT%'
          then 1 end)                             as total_order_reinstatements

from {{ ref('silver_dbt') }}
group by all