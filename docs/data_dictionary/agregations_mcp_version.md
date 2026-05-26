# Table: agregations
**Type:** aggregate
**Description:** Aggregated person-level summary table containing anonymized demographic attributes, charge information, and rolled-up immigration enforcement and removal metrics. Each row represents one anonymized individual with counts and date ranges derived from underlying event-level records.
**Primary Key:** anonymized_identifier
**Grain:** One row per anonymized individual identifier.
**Source System:** Derived/aggregated from underlying immigration enforcement and removal event source data.

## Columns
### anonymized_identifier
- **Type:** VARCHAR
- **Description:** An anonymized unique identifier representing an individual across aggregated records.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** False
- **Example values:** a1f9c23e, anon_10452, 7d88b1aa
- **Gotchas:** Expected to be unique at this table's grain. Although anonymized, treat as sensitive quasi-identifier because it links all subject-level metrics.

### gender
- **Type:** VARCHAR
- **Description:** Reported gender associated with the anonymized individual.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** Male, Female, Unknown
- **Gotchas:** May be null, unknown, inconsistent across source records, or reflect source-system coding rather than self-identification.

### birth_year
- **Type:** DOUBLE
- **Description:** Year of birth for the individual, stored as a numeric value.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** 1984, 1997, 1972
- **Gotchas:** Stored as DOUBLE instead of integer, so downstream models may need casting. Nulls and implausible years should be validated before analysis.

### birth_country
- **Type:** VARCHAR
- **Description:** Country of birth associated with the individual.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** Mexico, Guatemala, El Salvador
- **Gotchas:** Country values may have inconsistent spelling, abbreviations, or historical naming depending on source standardization.

### citizenship_country
- **Type:** VARCHAR
- **Description:** Country of citizenship associated with the individual.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** Mexico, Honduras, India
- **Gotchas:** May differ from birth_country. Nulls, multiple citizenship cases, or source recoding may affect interpretation.

### criminal_charge
- **Type:** VARCHAR
- **Description:** Primary or associated criminal charge category linked to the individual in source records.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** DUI, Drug Possession, Assault
- **Gotchas:** Charge definitions may vary by jurisdiction and source record. An aggregate table may not preserve all historical charges if multiple exist.

### criminal_charge_status
- **Type:** VARCHAR
- **Description:** Status of the associated criminal charge, such as pending, convicted, or dismissed.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** Pending, Convicted, Dismissed
- **Gotchas:** Status may change over time, and this aggregate may only reflect one status rather than a full charge lifecycle.

### total_removal_events
- **Type:** BIGINT
- **Description:** Total count of removal events recorded for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 0, 1, 4
- **Gotchas:** Definition of a removal event depends on source logic. Verify whether repeated administrative updates are excluded from the count.

### earliest_removal_date
- **Type:** TIMESTAMP
- **Description:** Timestamp of the earliest recorded removal event for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2012-03-14 00:00:00, 2018-11-02 13:45:00, 2020-01-09 09:30:00
- **Gotchas:** Null when no removal exists. Time component may be artificial or source-dependent; many analyses should truncate to date.

### most_recent_removal_date
- **Type:** TIMESTAMP
- **Description:** Timestamp of the most recent recorded removal event for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 2019-06-21 00:00:00, 2023-08-17 16:20:00, 2021-12-31 23:59:59
- **Gotchas:** Null when no removal exists. Should be on or after earliest_removal_date; if not, source or transformation issues may exist.

### total_years_with_removals
- **Type:** BIGINT
- **Description:** Number of distinct calendar years in which the individual had at least one recorded removal.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 0, 1, 3
- **Gotchas:** This is not necessarily the span between earliest and latest removal dates; it counts distinct years with activity.

### total_distinct_departure_ports
- **Type:** BIGINT
- **Description:** Count of distinct departure ports associated with the individual's recorded removals or returns.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 2, 5
- **Gotchas:** Port names may require normalization upstream; otherwise counts can be inflated by spelling or coding differences.

### total_distinct_enforcement_programs
- **Type:** BIGINT
- **Description:** Count of distinct enforcement programs linked to the individual's records.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 1, 3, 6
- **Gotchas:** Program definitions may change over time, so distinct counts can reflect historical coding changes rather than meaningful program diversity.

### estimated_current_age
- **Type:** DOUBLE
- **Description:** Estimated current age of the individual, likely derived from birth_year and the current or reference year.
- **Nullable:** True
- **PII:** True
- **Used for aggregations:** True
- **Example values:** 27, 41, 63
- **Gotchas:** Stored as DOUBLE and likely time-relative, so values can change as of run date. Prefer recalculating from birth_year and a fixed reference date for reproducibility.

### total_voluntary_returns
- **Type:** BIGINT
- **Description:** Total count of voluntary return events recorded for the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 0, 2, 7
- **Gotchas:** Business rules for what qualifies as a voluntary return should be validated against source definitions.

### total_order_reinstatements
- **Type:** BIGINT
- **Description:** Total count of removal order reinstatement events associated with the individual.
- **Nullable:** True
- **PII:** False
- **Used for aggregations:** True
- **Example values:** 0, 1, 3
- **Gotchas:** Ensure analysts distinguish reinstatements from removals; these may be related but not interchangeable event types.
