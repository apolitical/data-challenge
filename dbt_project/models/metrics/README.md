# Metrics Layer

Build your metric models here (Tasks 1 & 2).

This layer produces time-series aggregations and KPIs built on top of your intermediate or base models.

## Hints

- Task 1 asks for a daily time-series — think about what grain and joins are needed to produce gap-free output
- Task 2 asks for a cohort-based analysis — think about how to group users and track them over time
- Reference your intermediate and marts models with `{{ ref(...) }}`
- Materialized as `table` (configured in dbt_project.yml)
- Add a `schema.yml` with tests
