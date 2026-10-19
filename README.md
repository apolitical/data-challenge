# Apolitical Senior Data Engineer challenge

A member engagement pipeline that has already had an incident. The exercise is to make
it safe to run.

**Start with [`SENIOR_DATA_ENGINEER.md`](SENIOR_DATA_ENGINEER.md).** Then
[`SETUP.md`](SETUP.md) to get it running.

| | |
|---|---|
| Warehouse | DuckDB, a local file |
| Transformation | dbt Core |
| Orchestration | Airflow 2.11, optional: `scripts/run_pipeline.py` runs the same tasks without it |
| CRM | a local mock with a documented API contract |
| Python | 3.11 or 3.12, with `uv` |

No Docker and no cloud credentials: **nothing in the pipeline can reach a real service.**

```
SENIOR_DATA_ENGINEER.md     the exercise
SETUP.md                    how to run everything
MIGRATION_MANIFEST.md       status of the upstream engagement feed migration
DUCKDB_REFERENCE.md         DuckDB syntax you may not have used before
dbt-project/                models, tests, dbt profile
airflow-project/dags/       the sync-member-engagement DAG and its CRM client
airflow-project/utilities/  shared helpers: runtime, pipeline config, failure alerting
mock_crm/                   the mock CRM, its API contract and its state
mock_data/                  committed fixtures, one input snapshot per run date
scripts/                    warehouse loading, the pipeline run without Airflow, incident
                            replay, reset, data exploration
default.env                 environment for running the DAG under Airflow
```
