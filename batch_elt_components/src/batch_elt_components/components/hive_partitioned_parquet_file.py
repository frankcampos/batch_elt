import dagster as dg
from dagster import asset, AssetIn, Output
import duckdb
from dagster_duckdb import DuckDBResource

class HivePartitionedParquetFile(dg.Component, dg.Model, dg.Resolvable):
    """COMPONENT SUMMARY HERE.

    COMPONENT DESCRIPTION HERE.
    """
    output_path: str

    # added fields here will define params when instantiated in Python, and yaml schema via Resolvable

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Add definition construction logic here.
        @asset(
            name="partitioned_removals",
            group_name="bronze_components",
            key_prefix=["bronze_components"],
            ins={
            "removals_latest": AssetIn(key=["bronze_components", "parquetfile_removals_latest"]),
            "removals_2012_2023": AssetIn(key=["bronze_components", "parquetfile_ice_removals_2012_2023"]),
        },
        )
        def _asset(database:DuckDBResource,removals_latest, removals_2012_2023):
            output_path = self.output_path
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
        FROM read_parquet('data/raw/removals_latest/*.parquet')

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
        FROM read_parquet('data/raw/ice_removals_2012_2023/*.parquet')
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


        return dg.Definitions(assets=[_asset])
