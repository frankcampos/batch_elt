# ICE Removals Data Dictionary

This dictionary covers the unified schema produced by merging two source datasets:
- **Historical (FY2012–2023):** `ice-removals-2012-2023/*.parquet`
- **Modern (Latest):** `removals-latest.parquet`

---

## Unified / Output Schema

| Column Name | Data Type | Description | Source Mapping |
|---|---|---|---|
| `departure_date` | `TIMESTAMP` | Date the individual departed the United States. | Table 1: `departed_date` → Table 2: `departure_date` |
| `port_of_departure` | `VARCHAR` | Name of the port or airport through which the individual departed (e.g., `NOGALES, AZ, POE`). | Both tables: `port_of_departure` |
| `departure_country` | `VARCHAR` | Country to which the individual was removed. | Both tables: `departure_country` |
| `case_status` | `VARCHAR` | Final status of the removal case (e.g., `6-Deported/Removed - Deportability`, `8-Excluded/Removed - Inadmissibility`). | Both tables: `case_status` |
| `case_category` | `VARCHAR` | Legal category/procedure under which the case was processed (e.g., `[8F] Expedited Removal`, `[16] Reinstated Final Order`). | Both tables: `case_category` |
| `final_order_yes_no` | `VARCHAR` | Indicates whether a final order of removal was issued (`YES` / `NO`). | Both tables: `final_order_yes_no` |
| `final_order_date` | `TIMESTAMP` | Date the final order of removal was issued. | Both tables: `final_order_date` |
| `gender` | `VARCHAR` | Gender of the individual (`Male` / `Female`). | Both tables: `gender` |
| `birth_country` | `VARCHAR` | Country where the individual was born. | Both tables: `birth_country` |
| `citizenship_country` | `VARCHAR` | Country of which the individual is a citizen. | Both tables: `citizenship_country` |
| `birth_year` | `DOUBLE` | Year of birth of the individual. | Both tables: `birth_year` |
| `entry_status` | `VARCHAR` | Immigration status at the time of entry (e.g., `PWA Mexico`, `Legal Permanent Resident`, `Not Applicable`). | Both tables: `entry_status` |
| `entry_date` | `TIMESTAMP` | Date the individual entered the United States. | Both tables: `entry_date` |
| `msc_charge` | `VARCHAR` | Most serious criminal charge on record (e.g., `Drug Trafficking`, `Domestic Violence`). | Table 1: `msc_ncic_charge` → Table 2: `msc_charge` |
| `msc_charge_date` | `TIMESTAMP` | Date the most serious criminal charge was filed. | Both tables: `msc_charge_date` |
| `msc_charge_code` | `VARCHAR` | NCIC numeric code corresponding to the most serious criminal charge (e.g., `35AB`, `0301`). | Table 1: `msc_ncic_charge_code` → Table 2: `msc_charge_code` |
| `msc_conviction_date` | `TIMESTAMP` | Date of conviction for the most serious criminal charge. | Both tables: `msc_conviction_date` |
| `msc_charge_status` | `VARCHAR` | Status of the most serious criminal charge (e.g., `Convicted`). | Table 1: `msc_criminal_charge_status` → Table 2: `msc_criminal_charge_status` |
| `case_threat_level` | `DOUBLE` | Numeric threat level assigned to the case. Higher values indicate greater assessed threat. | Both tables: `case_threat_level` |
| `processing_disposition` | `VARCHAR` | Description of the legal processing method used (e.g., `Expedited Removal (I-860)`, `REINSTATEMENT OF DEPORT ORDER I-871`). | Both tables: `processing_disposition` |
| `current_program` | `VARCHAR` | ICE enforcement program responsible for the case at time of removal (e.g., `Border Patrol`, `ERO Criminal Alien Program`). | Table 1: `final_program` → Table 2: `current_program` |
| `apprehension_date` | `TIMESTAMP` | Date the individual was apprehended. | Table 1: `latest_person_apprehension_date` → Table 2: `apprehension_date` |
| `charge_section_code` | `VARCHAR` | INA section code for the charge (e.g., `212a7AiI`, `212a9CiII`). References the Immigration and Nationality Act. | Table 1: `final_charge_section_code` → Table 2: `charge_section_code` |
| `charge_code` | `VARCHAR` | Short charge code (e.g., `I7A1`, `I9C2`, `I6A`). | Table 1: `final_charge_code` → Table 2: `charge_code` |
| `anonymized_identifier` | `VARCHAR` | SHA-1 anonymized hash uniquely identifying an individual across records. Note: Table 2 source contains a typo (`anonymized_identifer`). | Table 1: `unique_identifier` → Table 2: `anonymized_identifer` |
| `year` | `INTEGER` | Derived column. Calendar year extracted from `departure_date`. Used as the Parquet partition key. | Derived: `year(departure_date)` |

---

## Fields Present in Modern Schema Only (not carried forward)

These columns exist in `removals-latest.parquet` but were excluded from the unified output.

| Column Name | Description |
|---|---|
| `docket_aor` | Area of Responsibility associated with the case docket (e.g., `Phoenix Area of Responsibility`). |
| `apprehension_state` | U.S. state where the individual was apprehended. |
| `apprehension_county` | U.S. county where the individual was apprehended. |
| `msc_criminal_charge_status_code` | Single-character code for the criminal charge status (e.g., `C` for Convicted). |
| `case_criminality` | Text label for criminality category (e.g., `1 Convicted Criminal`, `3 Other Immigration Violator`). |
| `felon` | Indicates whether the individual was classified as an aggravated felon. |
| `processing_disposition` (code) | Short code for processing disposition (e.g., `REINSTATEMENT`, `ER`). Present as `processing_disposition_code` in Table 2. |
| `case_category_time_of_arrest` | Case category recorded at the time of arrest, which may differ from the final case category. |
| `latest_arrest_program_current` | Name of the ICE program at the time of the most recent arrest. |
| `latest_arrest_program_current_code` | Code for the ICE program at the time of the most recent arrest. |
| `prior_deport_yes_no` | Indicates whether the individual had a prior deportation (`YES` / `NO`). |
| `latest_person_departed_date` | Most recent departure date on record for the individual. |
| `duplicate_likely` | Flag indicating whether the record is likely a duplicate (`0` = not a duplicate). |
| `file_original` | Name of the source file from which the record originated. |
| `sheet_original` | Sheet name within the source Excel file. |
| `row_original` | Row number in the source Excel file. |

---

## Notes

- **Partitioning:** The output Bronze layer is partitioned by `year` (derived from `departure_date`) for efficient downstream querying.
- **Schema normalization:** Column names were standardized across both source schemas during the `UNION ALL` merge. See the "Source Mapping" column above for the original field names.
- **Typo in source data:** The historical schema contains `anonymized_identifer` (missing an `i`). This is corrected to `anonymized_identifier` in the unified output.
- **Threat level:** `case_threat_level` values of `1.0` and `3.0` appear in the data; lower values may indicate higher threat priority based on ICE classification conventions.