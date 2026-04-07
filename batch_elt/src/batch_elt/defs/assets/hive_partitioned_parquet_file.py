import duckdb 
from dagster import asset, AssetIn, Output
from pathlib import Path
from dagster_duckdb import DuckDBResource

def create_partitioned_removals_asset():

    @asset(
        name="partitioned_removals",
        group_name="bronze",
        ins={
            "removals_latest": AssetIn(key=["parquet_files", "parquet_removals-latest"]),
            "removals_2012_2023": AssetIn(key=["parquet_files", "parquet_ice-removals-2012-2023"]),
        },
    )
    def _partitioned_removals(
            removals_latest: list[str],
            removals_2012_2023: list[str],
            database: DuckDBResource
        ) -> Output[str]:
        
        output_path = "data/bronze/removals"
        query = f'''
            COPY (
    SELECT
        *,
        year(departure_date) AS year
    FROM (
        -- Table 1 (Modern Schema - removals-latest)
        SELECT
            departed_date AS departure_date,
            port_of_departure,
            departure_country,
            case_status,
            case_category,
            final_order_yes_no,
            final_order_date,
            gender,
            birth_country,
            citizenship_country,
            birth_year,
            entry_status,
            entry_date,
            msc_ncic_charge AS msc_charge,
            msc_charge_date,
            msc_ncic_charge_code AS msc_charge_code,
            msc_conviction_date,
            msc_criminal_charge_status AS msc_charge_status,
            case_threat_level,
            processing_disposition,
            final_program AS current_program,
            latest_person_apprehension_date AS apprehension_date,
            final_charge_section_code AS charge_section_code,
            final_charge_code AS charge_code,
            unique_identifier AS anonymized_identifier
        FROM read_parquet('data/raw/removals-latest/*.parquet')

        UNION ALL

        -- Table 2 (Historical Schema - ice-removals-2012-2023)
        SELECT
            departure_date,
            port_of_departure,
            departure_country,
            case_status,
            case_category,
            final_order_yes_no,
            final_order_date,
            gender,
            birth_country,
            citizenship_country,
            birth_year,
            entry_status,
            entry_date,
            msc_charge,
            msc_charge_date,
            msc_charge_code,
            msc_conviction_date,
            msc_criminal_charge_status AS msc_charge_status,
            case_threat_level,
            processing_disposition,
            current_program,
            apprehension_date,
            charge_section_code,
            charge_code,
            anonymized_identifer AS anonymized_identifier
        FROM read_parquet('data/raw/ice-removals-2012-2023/*.parquet')
    ) AS combined_data
)
TO 'data/bronze' (
    FORMAT PARQUET,
    PARTITION_BY (year),
    OVERWRITE_OR_IGNORE 1
);
'''     
        with database.get_connection() as conn:
            conn.execute(query)

        return Output(
            value=output_path,
            metadata={
                "path": output_path,
                "format": "parquet",
                "partitioned_by": "year",
            },
        )

    return _partitioned_removals