# Metrics Layer

Build your metric models here (Tasks 2 & 3).

This layer produces time-series aggregations and KPIs built on top of your intermediate or marts models.

## Hints

- Task 2 asks for a daily time-series — think about what grain and joins are needed to produce gap-free output
- Task 3 asks for a cohort-based analysis — think about how to group users and track them over time
- Reference your intermediate and marts models with `{{ ref(...) }}`
- Materialized as `table` (configured in dbt_project.yml)
- Add a `schema.yml` with tests
