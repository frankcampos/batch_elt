# Table: agregations
**Type:** aggregate
**Description:** Aggregated person-level summary table containing anonymized demographic attributes, enforcement context, and rolled-up counts and dates related to removal events, voluntary returns, and order reinstatements.
**Primary Key:** anonymized_identifier
**Grain:** One row per anonymized individual identifier and associated attribute combination in the aggregated source.
**Source System:** Unknown; likely derived from immigration/enforcement event source data.

## Columns
### anonymized_identifier
- **Type:** VARCHAR
- **Description:** An anonymized unique identifier representing an individual across aggregated records.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** False
- **Example values:** a1f9c23d, anon_104582, 7b8e19ff
- **Gotchas:** Although anonymized, this acts like a person key and may still be considered sensitive in some contexts. Confirm whether each identifier appears only once in this table.

### gender
- **Type:** VARCHAR
- **Description:** Reported or inferred gender associated with the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Male, Female, Unknown
- **Gotchas:** May contain nulls, unknown values, inconsistent coding, or values that change over time if sourced from multiple systems.

### birth_year
- **Type:** DOUBLE
- **Description:** Year of birth for the individual.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** 1984, 1997, 1972
- **Gotchas:** Stored as DOUBLE rather than integer, so decimal artifacts may appear. Birth year can be sensitive quasi-identifying information.

### birth_country
- **Type:** VARCHAR
- **Description:** Country where the individual was born.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** Mexico, Guatemala, El Salvador
- **Gotchas:** Country names may not be standardized and may differ from citizenship_country.

### citizenship_country
- **Type:** VARCHAR
- **Description:** Country of citizenship associated with the individual.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** Mexico, Honduras, India
- **Gotchas:** Can differ from birth_country and may change over time depending on source data timing.

### criminal_charge
- **Type:** VARCHAR
- **Description:** Criminal charge category or description associated with the individual's record.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** DUI, Drug Possession, Assault
- **Gotchas:** May be free text or inconsistently categorized. A person may have multiple charges over time, so this field may not represent a complete history.

### criminal_charge_status
- **Type:** VARCHAR
- **Description:** Status of the associated criminal charge, such as pending, convicted, or dismissed.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** Pending, Convicted, Dismissed
- **Gotchas:** Status definitions may vary by jurisdiction or source. If multiple charges exist, status may reflect only one charge or a non-deterministic aggregation choice.

### total_removal_events
- **Type:** BIGINT
- **Description:** Total number of removal events recorded for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 0, 1, 4
- **Gotchas:** Null may mean unknown rather than zero. Confirm whether repeated events on the same date are counted separately.

### earliest_removal_date
- **Type:** TIMESTAMP
- **Description:** Timestamp of the earliest recorded removal event for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2012-03-14 00:00:00, 2018-09-01 13:45:00
- **Gotchas:** May include time components or default midnight timestamps depending on source precision. Time zone handling may not be standardized.

### most_recent_removal_date
- **Type:** TIMESTAMP
- **Description:** Timestamp of the most recent recorded removal event for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2021-11-20 00:00:00, 2024-01-05 08:30:00
- **Gotchas:** Should be greater than or equal to earliest_removal_date when both are present; validate for data quality issues.

### total_years_with_removals
- **Type:** BIGINT
- **Description:** Count of distinct calendar years in which the individual had at least one removal event.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 2, 5
- **Gotchas:** This is distinct years with activity, not elapsed years between first and last event.

### total_distinct_departure_ports
- **Type:** BIGINT
- **Description:** Number of distinct departure ports associated with the individual's removal events.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 3, 7
- **Gotchas:** Distinct counts depend on port standardization; duplicated or differently coded ports can inflate counts.

### total_distinct_enforcement_programs
- **Type:** BIGINT
- **Description:** Number of distinct enforcement programs linked to the individual's events.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 2, 4
- **Gotchas:** Program naming or code normalization materially affects distinct counts.

### estimated_current_age
- **Type:** DOUBLE
- **Description:** Estimated current age derived from birth year and a reference date.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** 27, 41, 63
- **Gotchas:** Stored as DOUBLE and likely derived, so values may be approximate and can become stale over time depending on refresh cadence.

### total_voluntary_returns
- **Type:** BIGINT
- **Description:** Total number of voluntary return events recorded for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 0, 1, 6
- **Gotchas:** Definition of voluntary return may differ from removal event definitions; null may not equal zero.

### total_order_reinstatements
- **Type:** BIGINT
- **Description:** Total number of times a prior order was reinstated for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 0, 1, 3
- **Gotchas:** Business logic for what qualifies as a reinstatement should be validated with source documentation.
