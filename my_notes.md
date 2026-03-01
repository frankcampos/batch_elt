# Immigration Removal Data Pipeline (dbt)

## Project Overview

This project unifies historical removal records from multiple CSV sources into a single, standardized reporting table.

## Source Statistics & Data Gap Analysis

The two source files have significantly different schemas, requiring a staging layer to align them before joining.

| Statistic        | query_result.csv | query_result_second.csv | Difference |
| :--------------- | :--------------- | :---------------------- | :--------- |
| **Row Count**    | 100              | 100                     | 0          |
| **Column Count** | 41               | 29                      | 12         |

### Schema Differences

To achieve a "unified table," the following major differences were resolved:

- **File 1 Only (Unique Attributes):** `docket_aor`, `apprehension_state`, `case_criminality`, `felon`, and `duplicate_likely`. These were excluded or handled as optional fields.
- **Naming Divergence:** - `departed_date` (File 1) vs `departure_date` (File 2).
  - `unique_identifier` (File 1) vs `anonymized_identifer` (File 2).
  - `msc_ncic_charge` (File 1) vs `msc_charge` (File 2).
- **Metadata:** File 1 uses `file_original`/`sheet_original` while File 2 uses `file`/`sheet`.

---

## Unified Table Schema

The final model, `fct_unified_removals`, consists of **28 standardized columns**.

### Core Transformation Logic

- **Normalization:** Standardizes source-specific names into a master convention.
- **Union:** Combines the 100 rows from each file into a single 200-row table.
- **Traceability:** Adds a `source_dataset` column to track the origin of every row.

---

## Data Dictionary

| Column Name                    | Type      | Description                                                      |
| :----------------------------- | :-------- | :--------------------------------------------------------------- |
| **departure_date**             | TIMESTAMP | The date the removal or departure was finalized.                 |
| **port_of_departure**          | STRING    | Location (POE) where departure occurred.                         |
| **departure_country**          | STRING    | The country to which the individual was removed.                 |
| **case_status**                | STRING    | The legal status of the case (e.g., Deported, Excluded).         |
| **case_category**              | STRING    | The legal authority/program used (e.g., Reinstated Final Order). |
| **gender**                     | STRING    | Gender of the individual.                                        |
| **birth_country**              | STRING    | Country of birth.                                                |
| **citizenship_country**        | STRING    | Country of legal citizenship.                                    |
| **birth_year**                 | INTEGER   | Year of birth.                                                   |
| **entry_date**                 | TIMESTAMP | Date of the individual's most recent entry.                      |
| **entry_status**               | STRING    | Legal classification at entry.                                   |
| **msc_charge**                 | STRING    | Primary criminal/administrative charge description.              |
| **msc_charge_code**            | STRING    | Alphanumeric code for the charge.                                |
| **msc_charge_date**            | DATE      | The date the charge was recorded.                                |
| **msc_criminal_charge_status** | STRING    | Status of the charge (e.g., Convicted, Pending).                 |
| **msc_conviction_date**        | DATE      | The date of legal conviction.                                    |
| **case_threat_level**          | FLOAT     | Priority ranking (1, 2, or 3) based on severity.                 |
| **processing_disposition**     | STRING    | The legal document issued (e.g., I-860, I-871).                  |
| **final_order_yes_no**         | STRING    | Indicates if a final order was issued (YES/NO).                  |
| **final_order_date**           | DATE      | The date the final removal order was signed.                     |
| **charge_code**                | STRING    | The final administrative charge code.                            |
| **charge_section_code**        | STRING    | The INA (Immigration and Nationality Act) section code.          |
| **apprehension_date**          | TIMESTAMP | The date the individual was taken into custody.                  |
| **identifier**                 | STRING    | Anonymized unique ID for tracking individuals.                   |
| **file_name**                  | STRING    | Original source file name for auditing.                          |
| **sheet_name**                 | STRING    | Original tab name.                                               |
| **row_number**                 | INTEGER   | Original row index from the source file.                         |
| **source_dataset**             | STRING    | The dbt source name (`query_result` or `query_result_second`).   |

---

## Technical Maintenance

To update this data, run:
`dbt run --select fct_unified_removals`
