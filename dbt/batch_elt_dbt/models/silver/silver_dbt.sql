{{config(
    materialized = 'table',
    database = 'my_ducklake'
)}}

select
    -- identifiers
    anonymized_identifier,
    year,

    -- removal event details
    departure_date,
    port_of_departure                           as departure_port,
    departure_country,
    apprehension_date,

    -- case details
    case_status,
    case_category,
    final_order_yes_no                          as has_final_order,
    final_order_date,
    case_threat_level                           as threat_level,
    processing_disposition                      as disposition,
    charge_section_code,
    charge_code,

    -- program
    current_program                             as enforcement_program,

    -- personal demographics
    gender,
    birth_year,
    birth_country,
    citizenship_country,

    -- entry info
    entry_status,
    entry_date,

    -- criminal charge
    msc_charge                                  as criminal_charge,
    msc_charge_date                             as criminal_charge_date,
    msc_charge_code                             as criminal_charge_code,
    msc_conviction_date                         as criminal_conviction_date,
    msc_charge_status                           as criminal_charge_status

from {{ ref('consolidated_removals') }}