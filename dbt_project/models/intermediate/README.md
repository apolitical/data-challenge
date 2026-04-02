# Intermediate Layer

Build reusable intermediate models here.

This layer sits between base and marts/metrics — transform and reshape data into 
grains that are useful for downstream aggregations.

## Hints

- What grain would make it easy to build the downstream models?
- Reference your base or other intermediate models with `{{ ref(...) }}`
- By default, this layer is materialized as `view` (configured in dbt_project.yml)
