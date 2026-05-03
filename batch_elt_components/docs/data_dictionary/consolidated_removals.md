# Table: consolidated_removals
**Type:** fact
**Description:** This table contains consolidated information about removals, including details about the departure, case status, demographics, and charges associated with individuals being removed.
**Primary Key:** anonymized_identifier
**Grain:** One row per removal case.
**Source System:** Immigration Removal System

## Columns
### departure_date
- **Type:** TIMESTAMP
- **Description:** The date and time when the removal took place.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2023-01-15 08:30:00, 2023-06-20 17:45:00
- **Gotchas:** Make sure to convert to UTC for consistency.

### port_of_departure
- **Type:** VARCHAR
- **Description:** The port from which the individual was removed.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Los Angeles, New York, Miami
- **Gotchas:** Ensure proper handling of special characters in port names.

### departure_country
- **Type:** VARCHAR
- **Description:** Country where the individual was removed to.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Mexico, Canada, Honduras
- **Gotchas:** Data should match ISO country codes.

### case_status
- **Type:** VARCHAR
- **Description:** Current status of the removal case.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Completed, Pending, In Appeal
- **Gotchas:** Ensure consistent status terminology.

### case_category
- **Type:** VARCHAR
- **Description:** Category of the removal case, indicating the reason for removal.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Deportation, Voluntary Departure
- **Gotchas:** Categories should be defined in a lookup table.

### final_order_yes_no
- **Type:** VARCHAR
- **Description:** Indicates if a final order of removal was issued (Yes/No).
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** False
- **Example values:** Yes, No
- **Gotchas:** Ensure values are consistently 'Yes' or 'No'.

### final_order_date
- **Type:** TIMESTAMP
- **Description:** Date when the final order of removal was issued, if applicable.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2023-01-01 12:00:00, 2022-12-15 09:30:00
- **Gotchas:** May be null if no final order exists.

### gender
- **Type:** VARCHAR
- **Description:** Gender of the individual being removed.
- **Nullable:** False
- **PII:** True
- **Used for aggregations:** True
- **Example values:** Male, Female, Non-binary
- **Gotchas:** Standardize gender options.

### birth_country
- **Type:** VARCHAR
- **Description:** Country of birth of the individual.
- **Nullable:** False
- **PII:** True
- **Used for aggregations:** True
- **Example values:** USA, Vietnam, India
- **Gotchas:** Matches should be against ISO codes.

### citizenship_country
- **Type:** VARCHAR
- **Description:** Country of citizenship of the individual.
- **Nullable:** False
- **PII:** True
- **Used for aggregations:** True
- **Example values:** USA, Canada, Mexico
- **Gotchas:** Data must be validated for accuracy.

### birth_year
- **Type:** DOUBLE
- **Description:** Year of birth of the individual, should be a four-digit year.
- **Nullable:** False
- **PII:** True
- **Used for aggregations:** True
- **Example values:** 1990, 1985, 2000
- **Gotchas:** Should be a valid year.

### entry_status
- **Type:** VARCHAR
- **Description:** Status of the individual's entry into the country.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Legal, Illegal
- **Gotchas:** Ensure clarity of terms used in entry status.

### entry_date
- **Type:** TIMESTAMP
- **Description:** Date when the individual first entered the country.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2015-06-10 14:00:00, 2020-03-25 10:00:00
- **Gotchas:** Must not be later than departure_date.

### msc_charge
- **Type:** VARCHAR
- **Description:** Description of the charge leading to removal.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Illegal Entry, Fraud
- **Gotchas:** Should match defined charge descriptions.

### msc_charge_date
- **Type:** TIMESTAMP
- **Description:** Date when the charge was filed.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2022-01-15 08:30:00, 2022-01-20 09:00:00
- **Gotchas:** Should precede removal date.

### msc_charge_code
- **Type:** VARCHAR
- **Description:** Code associated with the charge.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** C001, C002
- **Gotchas:** Ensure consistency with charge codes.

### msc_conviction_date
- **Type:** TIMESTAMP
- **Description:** Date of conviction associated with the case, if applicable.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2022-02-10 12:00:00
- **Gotchas:** May be null if no conviction exists.

### msc_charge_status
- **Type:** VARCHAR
- **Description:** Current status of the charge.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Filed, Resolved, Dismissed
- **Gotchas:** Ensure consistent status values.

### case_threat_level
- **Type:** DOUBLE
- **Description:** Numeric value indicating the threat level of the case.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1.0, 2.5, 3.0
- **Gotchas:** Scale must be defined and consistently used.

### processing_disposition
- **Type:** VARCHAR
- **Description:** Disposition result of the case processing.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Approved, Denied
- **Gotchas:** Choices must be clearly defined.

### current_program
- **Type:** VARCHAR
- **Description:** Program under which the removal is being processed.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Removal Program, Voluntary Departure Program
- **Gotchas:** Ensure program names are standardized.

### apprehension_date
- **Type:** TIMESTAMP
- **Description:** Date when the individual was apprehended for the removal case.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2022-01-01 10:00:00
- **Gotchas:** Should precede entry date if they entered illegally.

### charge_section_code
- **Type:** VARCHAR
- **Description:** Code indicating the legal section under which the charge is made.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Sec. 245, Sec. 212
- **Gotchas:** Must match legal classification codes.

### charge_code
- **Type:** VARCHAR
- **Description:** Specific code associated with the legal charge.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** A-1, B-3
- **Gotchas:** Ensure accuracy in legal coding.

### anonymized_identifier
- **Type:** VARCHAR
- **Description:** A unique anonymized identifier for each removal case.
- **Nullable:** False
- **PII:** True
- **Used for aggregations:** False
- **Example values:** A1234568, B9876543
- **Gotchas:** Identifiers must remain consistent and unique.

### year
- **Type:** BIGINT
- **Description:** The year in which the removal took place.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2023, 2022
- **Gotchas:** Ensure this matches the year of departure date.
