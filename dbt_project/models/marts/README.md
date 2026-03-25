# Marts Layer

Build your star schema here (Task 1).

## Expected models

- `dim_users` — User dimension (one row per user)
- `dim_courses` — Course dimension (one row per course)
- `dim_dates` — Date dimension (one row per calendar date, enriched)
- `fct_events` — Event fact table (one row per event, with FK references)

## Tips

- Use `{{ ref('base_raw__users') }}` etc. to reference the provided base models
- Add a `schema.yml` with tests (unique, not_null, relationships)
- Materialized as `table` (configured in dbt_project.yml)
