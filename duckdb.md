SELECT \* FROM read_xlsx('data/raw/ice-removals-2012-2023.xlsx', all_varchar=true)
LIMIT 10;

-- This extracts the unique sheet names from your Excel file
SELECT DISTINCT sheet
FROM read_xlsx('data/raw/ice-removals-2012-2023.xlsx', all_varchar=true);

-- Indexing starts at 0, 1, 2. Try '0' for the first sheet.
SELECT \* FROM read_xlsx('data/raw/ice-removals-2012-2023.xlsx',
sheet=0,
all_varchar=true)

-- counts
SELECT count(\*) FROM read_xlsx('data/raw/ice-removals-2012-2023.xlsx',
sheet='Sheet 1',
all_varchar=true) ;

-- I neeed to use this query to create the files by years

COPY (
SELECT
_,
year(departure_date) AS year
-- The path is now a clean string with no backticks
FROM read_parquet('data/raw/ice-removals-2012-2023/_.parquet')
)
-- Also removed backticks from the output path
TO 'data/raw/bronze' (
FORMAT PARQUET,
PARTITION_BY (year),
OVERWRITE_OR_IGNORE 1
);

---

COPY (
SELECT
_,
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
FROM read_parquet('data/raw/removals-latest/_.parquet')

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
TO 'data/raw/bronze' (
FORMAT PARQUET,
PARTITION_BY (year),
OVERWRITE_OR_IGNORE 1
);
