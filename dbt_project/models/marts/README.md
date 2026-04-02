# Marts Layer

Build your final business-facing models here (Task 3, optional).

This layer should be optimised for consumption by BI tools and downstream analytics.

## Hints

- Consider how BI tools (e.g., ThoughtSpot, Looker, Tableau) typically expect data to be structured for efficient querying and self-serve analytics
- Think about star schema or snowflake schema — which is more appropriate here?
- What grain should each model have?
- Use `{{ ref('base_raw__users') }}` etc. to reference the provided base models
- Add a `schema.yml` with tests (unique, not_null, relationships)
- Materialized as `table` (configured in dbt_project.yml)
