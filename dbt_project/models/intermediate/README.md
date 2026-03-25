# Intermediate Layer

Build reusable intermediate models here (Task 2).

## Suggested models

- A daily user activity summary (one row per user per active date)

## Tips

- Reference your mart models with `{{ ref('fct_events') }}`
- This layer is materialized as `view` (configured in dbt_project.yml)
- Think about what grain will be most useful for downstream metrics
