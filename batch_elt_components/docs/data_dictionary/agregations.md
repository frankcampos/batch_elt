# Table: agregations
**Type:** aggregate
**Description:** Aggregate-level table summarizing removal and enforcement history by anonymized individual identifier. Each row represents one anonymized person and includes demographic attributes, criminal charge indicators, removal activity totals, distinct program and port counts, and derived age metrics.
**Primary Key:** anonymized_identifier
**Grain:** One row per anonymized_identifier
**Source System:** Derived aggregate table from enforcement/removal event source data

## Columns
### anonymized_identifier
- **Type:** varchar
- **Description:** An anonymized unique identifier for the individual represented in the aggregate record.
- **Nullable:** False
- **PII:** False
- **Used for aggregations:** False
- **Example values:** a1f9c2d8, anon_10452
- **Gotchas:** Anonymized but still unique at the person level; should be treated as a sensitive quasi-identifier in downstream sharing.

### gender
- **Type:** varchar
- **Description:** Reported gender associated with the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Male, Female, Unknown
- **Gotchas:** May contain nulls, unknown values, or inconsistent category labels depending on source data standardization.

### birth_year
- **Type:** float64
- **Description:** Year of birth for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1984, 1997, 1972
- **Gotchas:** Stored as float64 rather than integer, so downstream logic may need casting; may be approximate or missing.

### birth_country
- **Type:** varchar
- **Description:** Country where the individual was born.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Mexico, Guatemala, El Salvador
- **Gotchas:** Country names may require normalization for consistent grouping; nulls or legacy country names may appear.

### citizenship_country
- **Type:** varchar
- **Description:** Country of citizenship associated with the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Mexico, Honduras, United States
- **Gotchas:** May differ from birth_country; possible multiple business interpretations if citizenship changed over time.

### criminal_charge
- **Type:** varchar
- **Description:** Criminal charge category or description associated with the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Drug Offense, Assault, None
- **Gotchas:** May reflect only one charge, a normalized category, or a representative value from many events; confirm aggregation logic before analytical use.

### criminal_charge_status
- **Type:** varchar
- **Description:** Status of the criminal charge, such as pending, convicted, or dismissed.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** Convicted, Pending, Dismissed
- **Gotchas:** Status definitions may vary by source and time; not all records with criminal_charge will necessarily have a populated status.

### total_removal_events
- **Type:** int64
- **Description:** Total number of removal events recorded for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 3, 7
- **Gotchas:** Definition of a removal event should be validated against source business rules; may exclude certain departures or administrative actions.

### earliest_removal_date
- **Type:** timestamp
- **Description:** Timestamp of the earliest recorded removal event for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2011-04-15 00:00:00, 2018-09-03 14:22:10
- **Gotchas:** Timestamp precision and timezone handling may vary; may be null when no removal event exists.

### most_recent_removal_date
- **Type:** timestamp
- **Description:** Timestamp of the most recent recorded removal event for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2021-06-30 00:00:00, 2024-01-11 08:45:00
- **Gotchas:** Should be greater than or equal to earliest_removal_date; null when no removal event exists.

### total_years_with_removals
- **Type:** int64
- **Description:** Count of distinct calendar years in which the individual had at least one removal event.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 2, 5
- **Gotchas:** Represents distinct years, not elapsed years between first and last removal.

### total_distinct_departure_ports
- **Type:** int64
- **Description:** Number of distinct departure ports associated with the individual's removal events.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 4, 9
- **Gotchas:** Depends on source port standardization; duplicates may be overstated if codes and names are mixed.

### total_distinct_enforcement_programs
- **Type:** int64
- **Description:** Number of distinct enforcement programs associated with the individual's records.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 2, 6
- **Gotchas:** Program definitions may change over time, affecting comparability across periods.

### estimated_current_age
- **Type:** float64
- **Description:** Derived estimate of the individual's current age, typically calculated from birth_year relative to the current date or reporting date.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 27, 41, 63
- **Gotchas:** Because it is estimated and stored as float64, it may include rounding issues and can change over time as the reference date changes.

### total_voluntary_returns
- **Type:** int64
- **Description:** Total number of voluntary return events recorded for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 0, 2, 11
- **Gotchas:** Should not automatically be combined with removal totals unless business definitions explicitly support that.

### total_order_reinstatements
- **Type:** int64
- **Description:** Total number of times a prior order was reinstated for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 0, 1, 3
- **Gotchas:** Business logic for reinstatement counting may vary across source systems or over time.
