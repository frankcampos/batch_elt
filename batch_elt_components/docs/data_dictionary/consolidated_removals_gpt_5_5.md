# Table: consolidated_removals
**Type:** fact
**Description:** Consolidated case-level or event-level table describing removal-related departures, case status, demographic attributes, immigration entry information, charge information, processing disposition, and reporting year. The table appears intended for analysis of removals and related immigration enforcement outcomes over time.
**Primary Key:** No explicit primary key provided. `anonymized_identifier` may link records but should not be assumed unique; a composite key such as `anonymized_identifier`, `departure_date`, and case/event attributes may be required after validation.
**Grain:** One record per consolidated removal case or removal-related departure event for an anonymized individual; verify whether individuals can appear multiple times across cases or departures.
**Source System:** Consolidated removals dataset / immigration enforcement case-management source systems

## Columns
### departure_date
- **Type:** TIMESTAMP
- **Description:** Date and time when the removal departure occurred or was recorded.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2023-04-15 00:00:00, 2021-11-02 14:30:00
- **Gotchas:** May be null for pending, incomplete, or administratively closed cases. Use the `year` column for partition-style filtering if it is derived consistently from this date, but validate alignment first.

### port_of_departure
- **Type:** VARCHAR
- **Description:** Port, airport, border crossing, or other location from which the individual departed as part of the removal process.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Laredo, JFK Airport, Miami
- **Gotchas:** Values may not be standardized and can include abbreviations, misspellings, or varying naming conventions.

### departure_country
- **Type:** VARCHAR
- **Description:** Country from which the removal departure occurred.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** United States, Mexico, Canada
- **Gotchas:** May differ from destination, citizenship, or birth country. Country naming may not be normalized across records.

### case_status
- **Type:** VARCHAR
- **Description:** Current or final status of the immigration/removal case.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Closed, Pending, Removed, Terminated
- **Gotchas:** Status definitions may vary by source workflow. Avoid assuming this field alone determines whether a departure occurred; check `departure_date` and `processing_disposition` as well.

### case_category
- **Type:** VARCHAR
- **Description:** High-level category or classification of the case.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Removal, Return, Deportation, Expedited Removal
- **Gotchas:** Categories may reflect legal, operational, or source-system classifications and should be interpreted with business definitions.

### final_order_yes_no
- **Type:** VARCHAR
- **Description:** Indicator of whether the case had a final order of removal.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Yes, No, Y, N
- **Gotchas:** Stored as text rather than boolean and may contain inconsistent encodings or casing. Normalize before filtering.

### final_order_date
- **Type:** TIMESTAMP
- **Description:** Date and time when the final order of removal was issued or recorded.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2022-08-10 00:00:00, 2020-01-21 09:15:00
- **Gotchas:** May be null when `final_order_yes_no` is No, unknown, or inconsistently populated. Validate that it does not postdate departure for relevant analyses.

### gender
- **Type:** VARCHAR
- **Description:** Recorded gender of the individual associated with the case.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Male, Female, Unknown
- **Gotchas:** Sensitive demographic attribute. Values may be missing, historical, or limited to source-system categories.

### birth_country
- **Type:** VARCHAR
- **Description:** Country where the individual was born.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Mexico, Guatemala, Honduras
- **Gotchas:** May differ from `citizenship_country`. Country names may require normalization for consistent grouping.

### citizenship_country
- **Type:** VARCHAR
- **Description:** Country of citizenship or nationality recorded for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Mexico, El Salvador, China
- **Gotchas:** May differ from `birth_country`; individuals may have multiple citizenships but this field likely records only one value.

### birth_year
- **Type:** DOUBLE
- **Description:** Year of birth of the individual.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** 1985, 1997, 1972
- **Gotchas:** Stored as DOUBLE even though it represents a year. Cast to integer after handling nulls and invalid values. This is quasi-identifying demographic information when combined with other fields.

### entry_status
- **Type:** VARCHAR
- **Description:** Immigration or admission status at entry, as recorded in the case data.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** EWI, Visa Overstay, Lawful Permanent Resident, Visitor
- **Gotchas:** Codes and labels may require a reference table for interpretation. May represent alleged or recorded status rather than adjudicated status.

### entry_date
- **Type:** TIMESTAMP
- **Description:** Date and time when the individual entered the country, if known or recorded.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2018-06-01 00:00:00, 2020-12-18 13:45:00
- **Gotchas:** Often unknown or approximate. Check for dates after apprehension or departure, which may indicate data quality issues or different event definitions.

### msc_charge
- **Type:** VARCHAR
- **Description:** Most serious criminal charge or related charge description recorded for the case.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** DUI, Drug Trafficking, Assault, No Criminal Charge
- **Gotchas:** Charge descriptions may be sensitive and may reflect charges rather than convictions. Use with caution and avoid implying guilt unless supported by conviction fields.

### msc_charge_date
- **Type:** TIMESTAMP
- **Description:** Date and time associated with the most serious criminal charge.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2021-03-04 00:00:00, 2019-09-12 10:00:00
- **Gotchas:** May refer to charge filing, arrest, or source-system record date depending on source definitions.

### msc_charge_code
- **Type:** VARCHAR
- **Description:** Code corresponding to the most serious criminal charge.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 35A, 13B, 90Z
- **Gotchas:** Requires code definitions or a lookup table for accurate interpretation. Codes may vary by jurisdiction or source system.

### msc_conviction_date
- **Type:** TIMESTAMP
- **Description:** Date and time of conviction associated with the most serious criminal charge, if applicable.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2021-07-19 00:00:00, 2018-05-30 11:20:00
- **Gotchas:** Null does not necessarily mean no conviction; it may mean unknown or not recorded. Do not infer conviction solely from charge fields.

### msc_charge_status
- **Type:** VARCHAR
- **Description:** Status of the most serious criminal charge.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Convicted, Pending, Dismissed, Unknown
- **Gotchas:** May mix procedural statuses and outcomes. Normalize values before aggregate reporting.

### case_threat_level
- **Type:** DOUBLE
- **Description:** Numeric threat-level score or category assigned to the case.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 2, 3
- **Gotchas:** Stored as DOUBLE; confirm scale, meaning, and whether higher values represent greater threat before analysis. Sensitive classification field.

### processing_disposition
- **Type:** VARCHAR
- **Description:** Operational or administrative disposition describing how processing concluded or is currently classified.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Removed, Returned, Voluntary Departure, Released
- **Gotchas:** May overlap with `case_status` and `case_category` but should not be treated as equivalent without business rules.

### current_program
- **Type:** VARCHAR
- **Description:** Program or enforcement process currently associated with the case.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** ERO, ATD, Detained, Non-Detained
- **Gotchas:** Program names and eligibility can change over time. Historical values may not align with current program definitions.

### apprehension_date
- **Type:** TIMESTAMP
- **Description:** Date and time when the individual was apprehended or encountered in connection with the case.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2022-02-17 00:00:00, 2023-01-08 16:45:00
- **Gotchas:** May be earlier or later than other case milestones depending on event definitions and data entry timing.

### charge_section_code
- **Type:** VARCHAR
- **Description:** Legal section code associated with a charge in the case.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 237(a)(1)(B), 212(a)(6)(A)(i), 101(a)(43)
- **Gotchas:** Legal codes may require domain expertise to interpret. Multiple charges may be collapsed into a single record or value.

### charge_code
- **Type:** VARCHAR
- **Description:** General charge code associated with the case or immigration violation.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** INA237, INA212, CRIM
- **Gotchas:** May not uniquely identify the legal basis without `charge_section_code` or reference data.

### anonymized_identifier
- **Type:** VARCHAR
- **Description:** Pseudonymous identifier for the individual or case subject, used to link records without exposing a direct identifier.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** False
- **Example values:** a7f3c9d2e1, ID_00012345, 9b1f0a6c
- **Gotchas:** Although anonymized, it can still enable linkage and may be considered sensitive. Do not attempt re-identification; protect according to privacy policy.

### year
- **Type:** BIGINT
- **Description:** Year associated with the record, typically derived from `departure_date` or used as a reporting/partition year.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2021, 2022, 2023
- **Gotchas:** Confirm whether this is calendar year, fiscal year, departure year, or ingestion/reporting year before using for time-series analysis.
