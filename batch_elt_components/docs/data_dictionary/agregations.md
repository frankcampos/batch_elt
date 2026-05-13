# Table: agregations
**Type:** aggregate
**Description:** Aggregate table summarizing removal and enforcement-related history at the anonymized individual level. Each row represents a single anonymized person with demographic attributes, charge information, and rollups of removal, return, and reinstatement activity.
**Primary Key:** anonymized_identifier
**Grain:** One row per anonymized_identifier
**Source System:** Unknown; likely derived from enforcement/removal event source data

## Columns
### anonymized_identifier
- **Type:** VARCHAR
- **Description:** An anonymized unique identifier representing an individual across aggregated enforcement and removal records.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** False
- **Example values:** a1f9c2e7, anon_10452, 7d8e91ab
- **Gotchas:** Acts as the logical primary key, but nullability indicates source data may not enforce it strictly. Confirm uniqueness before relying on it as a true key.

### gender
- **Type:** VARCHAR
- **Description:** Reported or recorded gender of the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Male, Female, Unknown
- **Gotchas:** May contain nulls, unknown values, or inconsistent coding across source systems.

### birth_year
- **Type:** DOUBLE
- **Description:** Year of birth for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1987, 1995, 1972
- **Gotchas:** Stored as DOUBLE rather than integer, so decimal artifacts may appear. Validate and cast before using in age or cohort analysis.

### birth_country
- **Type:** VARCHAR
- **Description:** Country where the individual was born.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Mexico, Guatemala, El Salvador
- **Gotchas:** Country names may not be standardized and may include abbreviations, alternate spellings, or nulls.

### citizenship_country
- **Type:** VARCHAR
- **Description:** Country of citizenship recorded for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Mexico, Honduras, United States
- **Gotchas:** May differ from birth_country and may change over time in source records; this field likely reflects a selected or latest value in the aggregation.

### criminal_charge
- **Type:** VARCHAR
- **Description:** Criminal charge associated with the individual or their enforcement history.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Drug Possession, Assault, Immigration Violation
- **Gotchas:** May represent a single selected charge rather than all charges tied to the individual. Definitions and granularity may vary.

### criminal_charge_status
- **Type:** VARCHAR
- **Description:** Status of the criminal charge, such as pending, convicted, or dismissed.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Pending, Convicted, Dismissed
- **Gotchas:** Status values may be incomplete, outdated, or inconsistently coded. Interpret carefully in legal outcome analysis.

### total_removal_events
- **Type:** BIGINT
- **Description:** Total count of removal events associated with the anonymized individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 0, 1, 4
- **Gotchas:** Because this is an aggregate metric, it should not be summed across duplicate rows. Null may mean unknown rather than zero.

### earliest_removal_date
- **Type:** TIMESTAMP
- **Description:** Earliest known removal event timestamp for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2012-03-14 00:00:00, 2018-09-01 13:22:10
- **Gotchas:** Timestamp precision may be artificial if source data is date-only. Be cautious when interpreting time-of-day values.

### most_recent_removal_date
- **Type:** TIMESTAMP
- **Description:** Most recent known removal event timestamp for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2021-07-30 00:00:00, 2024-01-05 08:15:42
- **Gotchas:** Should be greater than or equal to earliest_removal_date when both are populated. Null may indicate no removal history or missing source data.

### total_years_with_removals
- **Type:** BIGINT
- **Description:** Number of distinct calendar years in which the individual had one or more removal events.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 3, 7
- **Gotchas:** This is not the same as elapsed years between earliest and most recent removal dates.

### total_distinct_departure_ports
- **Type:** BIGINT
- **Description:** Count of distinct departure ports involved in the individual's removal history.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 2, 5
- **Gotchas:** Distinct counts depend on source port standardization; inconsistent port naming can inflate values.

### total_distinct_enforcement_programs
- **Type:** BIGINT
- **Description:** Count of distinct enforcement programs associated with the individual's records.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 3, 6
- **Gotchas:** Program definitions may overlap or change over time, affecting distinct counts.

### estimated_current_age
- **Type:** DOUBLE
- **Description:** Estimated current age of the individual, likely derived from birth_year and the current or reference year.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 29, 41, 58
- **Gotchas:** Stored as DOUBLE and may drift over time if not recomputed regularly. Confirm the reference date used for the estimate.

### total_voluntary_returns
- **Type:** BIGINT
- **Description:** Total count of voluntary return events associated with the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 0, 2, 9
- **Gotchas:** Null may indicate unavailable source data rather than no events. Do not combine with removal counts unless definitions are intentionally aligned.

### total_order_reinstatements
- **Type:** BIGINT
- **Description:** Total count of order reinstatement events associated with the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 0, 1, 3
- **Gotchas:** Event definitions should be validated with source documentation, as reinstatement logic may vary by jurisdiction or operational process.
