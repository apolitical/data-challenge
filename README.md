# Apolitical Senior Data Engineer challenge

A member engagement pipeline that has already had an incident. The exercise is to make
it safe to run.

**Start with [`SENIOR_DATA_ENGINEER.md`](SENIOR_DATA_ENGINEER.md).** Then
[`SETUP.md`](SETUP.md) to get it running.

## What this is

| | |
|---|---|
| Warehouse | DuckDB, local file, no server |
| Transformation | dbt Core |
| Orchestration | Airflow 2.11, local and optional (`uv sync --extra airflow`) |
| CRM | a local mock with a documented API contract |
| Python | 3.11 or 3.12 |
| Package manager | `uv` |

No Docker, no cloud credentials. The CRM is a local mock with dummy
credentials — **nothing in the pipeline can reach a real service.**

## Layout

```
SENIOR_DATA_ENGINEER.md   the exercise
SETUP.md                  how to run everything
MIGRATION_MANIFEST.md     status of the upstream engagement feed migration
DUCKDB_REFERENCE.md       DuckDB syntax you may not have used before
default.env               local environment; copy to .env and edit
dbt/                      models, tests, dbt profile
airflow/dags/             the sync-member-engagement pipeline and its CRM client
airflow/utilities/        shared helpers: runtime, pipeline config, failure alerting
mock_crm/                 the mock CRM, its API contract and its state
mock_data/                committed fixtures
scripts/                  warehouse loading, the pipeline run without Airflow, incident
                          replay, reset, data exploration
```
