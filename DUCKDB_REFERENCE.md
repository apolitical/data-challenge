# SQL Reference — DuckDB, BigQuery & PostgreSQL

This challenge uses **DuckDB** as the local database. If you're coming from BigQuery or PostgreSQL, this reference maps common patterns across all three.

## Date & Time

| What | PostgreSQL | BigQuery | DuckDB |
|------|-----------|----------|--------|
| Generate date series | `generate_series(start, end, '1 day')` | `GENERATE_DATE_ARRAY(start, end)` | `generate_series(start, end, INTERVAL '1 day')` |
| Cast to date | `ts::DATE` | `DATE(ts)` | `ts::DATE` |
| Date arithmetic | `d - INTERVAL '27 days'` | `DATE_SUB(d, INTERVAL 27 DAY)` | `d - INTERVAL '27 days'` |
| Date diff | `end - start` (returns integer) | `DATE_DIFF(end, start, DAY)` | `DATE_DIFF('day', start, end)` |
| Truncate to week | `DATE_TRUNC('week', d)` — Mon | `DATE_TRUNC(d, WEEK)` — Sun | `DATE_TRUNC('week', d)` — Mon |
| Day of week | `EXTRACT(DOW FROM d)` — 0=Sun | `EXTRACT(DAYOFWEEK FROM d)` — 1=Sun | `DAYOFWEEK(d)` — 0=Sun |
| Day name | `TO_CHAR(d, 'Day')` | `FORMAT_DATE('%A', d)` | `DAYNAME(d)` |
| Month name | `TO_CHAR(d, 'Month')` | `FORMAT_DATE('%B', d)` | `MONTHNAME(d)` |
| Format date | `TO_CHAR(d, 'YYYYMMDD')` | `FORMAT_DATE('%Y%m%d', d)` | `STRFTIME(d, '%Y%m%d')` |
| Current date | `CURRENT_DATE` | `CURRENT_DATE()` | `CURRENT_DATE` |

## Aggregation

| What | PostgreSQL | BigQuery | DuckDB |
|------|-----------|----------|--------|
| Collect into array | `ARRAY_AGG(DISTINCT x)` | `ARRAY_AGG(DISTINCT x)` | `LIST(DISTINCT x)` |
| String aggregation | `STRING_AGG(x, ',')` | `STRING_AGG(x, ',')` | `STRING_AGG(x, ',')` |
| Flatten/unnest | `UNNEST(arr)` | `UNNEST(arr)` | `unnest(list_col)` |
| Array length | `array_length(arr, 1)` | `ARRAY_LENGTH(arr)` | `len(arr)` |
| Boolean OR | `BOOL_OR(x)` | `LOGICAL_OR(x)` | `BOOL_OR(x)` |
| Approx distinct | N/A (use extension) | `APPROX_COUNT_DISTINCT(x)` | `approx_count_distinct(x)` |

## Window Functions

| What | PostgreSQL | BigQuery | DuckDB |
|------|-----------|----------|--------|
| RANGE window on dates | `ORDER BY d RANGE BETWEEN '27 days' PRECEDING AND CURRENT ROW` | `ORDER BY UNIX_DATE(d) RANGE BETWEEN 27 PRECEDING AND CURRENT ROW` | `ORDER BY d RANGE BETWEEN INTERVAL '27 days' PRECEDING AND CURRENT ROW` |
| COUNT DISTINCT in window | Not supported | Not supported | Not supported — use `COUNT(DISTINCT CASE WHEN ... THEN id END)` |
| ROW_NUMBER | `ROW_NUMBER() OVER (...)` | `ROW_NUMBER() OVER (...)` | `ROW_NUMBER() OVER (...)` *(same)* |

## Patterns You'll Need

### Date spine (provided in `base_calendar__dates`)
```sql
-- DuckDB: generate a continuous series of dates
SELECT d::DATE AS calendar_date
FROM generate_series(DATE '2023-01-01', DATE '2023-12-31', INTERVAL '1 day') AS t(d);
```

### Rolling distinct count (for RAU metrics)
```sql
-- COUNT(DISTINCT x) OVER (...) doesn't work — use this pattern instead:
SELECT
    dates.calendar_date,
    COUNT(DISTINCT CASE
        WHEN activity.date >= dates.calendar_date - INTERVAL '6 days'
        THEN activity.user_id
    END) AS rolling_7d_users
FROM dates
LEFT JOIN activity
    ON activity.date BETWEEN dates.calendar_date - INTERVAL '6 days' AND dates.calendar_date
GROUP BY dates.calendar_date;
```

### Weekly cohort grouping (for retention)
```sql
-- Truncate signup date to Monday of that week
DATE_TRUNC('week', signup_date)

-- Count weeks between two dates
DATE_DIFF('week', cohort_week, activity_week)
```

### Enriching a date dimension
```sql
-- Useful date attributes
DAYOFWEEK(d)                          -- 0=Sun, 6=Sat
DAYNAME(d)                            -- 'Monday', 'Tuesday', etc.
DAYOFWEEK(d) IN (0, 6)               -- is_weekend
DATE_TRUNC('week', d)::DATE          -- week_start (Monday)
DATE_TRUNC('month', d)::DATE         -- month_start
MONTHNAME(d)                          -- 'January', 'February', etc.
STRFTIME(d, '%Y%m%d')                -- integer-friendly date key
```

## Gotchas

- `RANGE BETWEEN INTERVAL '27 days' PRECEDING` is inclusive of both endpoints — covers **28 days** total
- `COUNT(DISTINCT x) OVER (...)` does **not** work in DuckDB — use a join + `COUNT(DISTINCT CASE ...)` instead
- `generate_series` result needs aliasing: `generate_series(...) AS t(col_name)`
- `DATE_TRUNC('week', d)` returns **Monday** in DuckDB and PostgreSQL, but **Sunday** in BigQuery
- `DATE_DIFF('day', start, end)` — note DuckDB uses `('unit', start, end)` order, BigQuery uses `(end, start, unit)`
- DuckDB and PostgreSQL share most syntax (casts, intervals, `BOOL_OR`) — BigQuery is the outlier
