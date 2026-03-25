# Metrics Layer

Build your metric models here (Tasks 2 & 3).

## Expected models

- RAU metrics — Rolling Active Users (DAU/WAU/MAU), one row per calendar date
- Cohort retention — Weekly retention matrix, one row per (cohort_week, periods_since)

## Tips

- Reference the date dimension with `{{ ref('dim_dates') }}`
- Reference your intermediate daily activity model
- Materialized as `table` (configured in dbt_project.yml)
- Add a `schema.yml` with tests
