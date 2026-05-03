# Table: consolidated_removals
**Type:** fact
**Description:** This table contains records of consolidated removals, detailing information about individuals removed from a country, including their departure date, port, citizenship, and case status.
**Primary Key:** anonymized_identifier
**Grain:** One record per individual removal event
**Source System:** Immigration Enforcement Database

## Columns
### departure_date
- **Type:** TIMESTAMP
- **Description:** Date and time when the individual departed.
- **Nullable:** False
- **PII:** True
- **Used for aggregations:** False
- **Example values:** 2023-01-15 14:30:00, 2023-08-22 09:00:00
- **Gotchas:** Ensure time zone information is considered when processing.

### port_of_departure
- **Type:** VARCHAR
- **Description:** The port from which the individual departed.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** JFK Airport, Los Angeles Port
- **Gotchas:** Standardize port nomenclature.

### departure_country
- **Type:** VARCHAR
- **Description:** Country from which the individual departed.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** USA, Canada
- **Gotchas:** Verify country codes are consistent.

### case_status
- **Type:** VARCHAR
- **Description:** Current status of the removal case (e.g., pending, completed).
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** completed, pending
- **Gotchas:** Status values must match predefined categories.

### case_category
- **Type:** VARCHAR
- **Description:** Categorization of the removal case (e.g., voluntary, involuntary).
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** voluntary, involuntary
- **Gotchas:** Ensure proper categorization based on policy.

### final_order_yes_no
- **Type:** VARCHAR
- **Description:** Indicates whether there is a final order for removal (Yes/No).
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Yes, No
- **Gotchas:** Ensure only 'Yes' or 'No' values are recorded.

### final_order_date
- **Type:** TIMESTAMP
- **Description:** Date when the final order was issued.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** False
- **Example values:** 2023-01-10 10:00:00, 2023-07-05 15:45:00
- **Gotchas:** Date should reflect the actual order issuance.

### gender
- **Type:** VARCHAR
- **Description:** Gender of the individual being removed.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** Male, Female, Non-binary
- **Gotchas:** Ensure inclusivity in gender representation.

### birth_country
- **Type:** VARCHAR
- **Description:** The country of birth of the individual.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** False
- **Example values:** Mexico, Vietnam
- **Gotchas:** Consistent country codes required.

### citizenship_country
- **Type:** VARCHAR
- **Description:** Country where the individual holds citizenship.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** False
- **Example values:** USA, Canada
- **Gotchas:** Ensure recorded country aligns with individual's documentation.

### birth_year
- **Type:** DOUBLE
- **Description:** Year of birth of the individual, as a numeric value.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** 1980, 1995
- **Gotchas:** Ensure valid birth years are recorded.

### entry_status
- **Type:** VARCHAR
- **Description:** Current entry status of the individual (e.g., legal, illegal).
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** legal, illegal
- **Gotchas:** Review entry status definitions for accuracy.

### entry_date
- **Type:** TIMESTAMP
- **Description:** Date when the individual first entered the country.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** False
- **Example values:** 2015-02-20 08:00:00, 2019-11-10 12:30:00
- **Gotchas:** Make sure to confirm entry date aligns with records.

### msc_charge
- **Type:** VARCHAR
- **Description:** Description of the charge leading to removal.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** False
- **Example values:** Unlawful Entry, Fraud
- **Gotchas:** Standardize charge terminology across records.

### msc_charge_date
- **Type:** TIMESTAMP
- **Description:** Date associated with the charge leading to removal.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** False
- **Example values:** 2023-01-05 14:00:00, 2023-03-15 09:30:00
- **Gotchas:** Verify that charge date is reflective of legal action.

### msc_charge_code
- **Type:** VARCHAR
- **Description:** Code that identifies the specific charge.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** False
- **Example values:** UNA001, FRA002
- **Gotchas:** Ensure charge codes are up-to-date and documented.

### msc_conviction_date
- **Type:** TIMESTAMP
- **Description:** Date of conviction related to the charge, if applicable.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** False
- **Example values:** 2023-01-11 11:00:00, 2023-03-20 10:15:00
- **Gotchas:** Conviction date must align with case documents.

### msc_charge_status
- **Type:** VARCHAR
- **Description:** Current status of the charge (e.g., pending, resolved).
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** resolved, pending
- **Gotchas:** Ensure status terms are consistent across cases.

### case_threat_level
- **Type:** DOUBLE
- **Description:** Numerical representation of the threat level associated with the case, on a scale of 1-10.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 5, 8
- **Gotchas:** Leverage clear guidelines for threat level scoring.

### processing_disposition
- **Type:** VARCHAR
- **Description:** Current disposition of the case processing (e.g., completed, in progress).
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** completed, in-progress
- **Gotchas:** Ensure disposition terms match ongoing procedures.

### current_program
- **Type:** VARCHAR
- **Description:** Current program under which the individual is being processed (e.g., asylum, deportation).
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** deportation, asylum
- **Gotchas:** Confirm programs reflect applicable statuses.

### apprehension_date
- **Type:** TIMESTAMP
- **Description:** Date on which the individual was apprehended.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** False
- **Example values:** 2022-12-20 16:30:00, 2023-05-22 11:15:00
- **Gotchas:** Ensure apprehension date is accurately recorded.

### charge_section_code
- **Type:** VARCHAR
- **Description:** Code indicating the legal section under which the charge falls.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** False
- **Example values:** SEC001, SEC002
- **Gotchas:** Cross-reference codes with legal guidelines.

### charge_code
- **Type:** VARCHAR
- **Description:** Detailed code for the specific charge.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** False
- **Example values:** CHG001, CHG002
- **Gotchas:** Maintain an updated reference for charge codes.

### anonymized_identifier
- **Type:** VARCHAR
- **Description:** Unique identifier which anonymizes the individual's identity.
- **Nullable:** False
- **PII:** True
- **Used for aggregations:** False
- **Example values:** A123456789, B987654321
- **Gotchas:** Ensure no personal identifiers can be derived from this value.

### year
- **Type:** BIGINT
- **Description:** Year of the removal event.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2023, 2022
- **Gotchas:** Ensure consistency with the departure date.
