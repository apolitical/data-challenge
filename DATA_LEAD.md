# Data Challenge

Timebox: **60-90 minutes**

This exercise is designed to assess how you:

- reason about a messy SQL transformation
- turn that logic into clear dbt models
- add a small amount of testing and documentation
- orchestrate the flow in Airflow with the TaskFlow API

You do not need to make everything production-ready. We care more about clear thinking, sensible structure, and good trade-offs.

## Start Here

1. Install `uv`.
2. Run `uv sync`.
3. Run `uv run python scripts/init_db.py`.
4. If you want to explore the data interactively, run `uv run jupyter lab`.

`SETUP.md` has the full setup guide, dbt commands, and Airflow commands.

## Repository Map

- `mock_data/`: sample CSV inputs
- `1-sql/messy_course_engagement_duckdb.sql`: legacy query to inspect and refactor
- `2-dbt_project/`: dbt project skeleton
- `3-airflow/dags/pipeline.py`: Airflow DAG to complete
- `notebooks/example_data_exploration.ipynb`: optional exploration notebook

## Scenario

A stakeholder relies on the legacy query in `1-sql/messy_course_engagement_duckdb.sql` to understand course engagement.

The query works, but it is hard to maintain and extend. Your job is to turn it into a small, clearer analytics pipeline using dbt and Airflow.

## Important

You are **absolutely free to edit** `1-sql/messy_course_engagement_duckdb.sql` directly.

It is intentionally messy. Treat it as working scratch code, not as something you need to preserve. You can simplify it, rewrite parts of it, or reduce it to smaller pieces as you work out the logic.

## Task 1: Understand the Legacy Query

Open `1-sql/messy_course_engagement_duckdb.sql` and work out:

- which tables it uses
- the intended grain of the final result
- which metrics it is trying to calculate
- which modelling or SQL anti-patterns should be cleaned up

You do not need to write a long explanation. You only need enough understanding to justify the dbt structure you create in Task 2.

`notebooks/example_data_exploration.ipynb` is available if you want to inspect the raw data first.

## Task 2: Refactor into dbt Models

Use the skeleton in `2-dbt_project/` to build a simple layered dbt project:

- `models/base/`: base or staging models over the raw tables
- `models/intermediate/`: any intermediate transforms you need
- `models/marts/`: the final mart

The final mart should be **one row per course** and should include metrics such as:

- `learners`: distinct users enrolled
- `active_learners`: distinct users with events
- `total_quizzes_completed`
- `total_videos_completed`
- `first_activity`
- `last_activity`

Use `source()` and `ref()` so the model dependencies are clear.

## Task 3: Add Basic Tests and Documentation

In `2-dbt_project/models/`:

- add `schema.yml` files, or extend the existing ones
- add `unique` and `not_null` tests on suitable keys for at least one or two models
- add a `relationships` test where it makes sense
- add short descriptions for the final mart and a few important columns

Keep this lightweight. We are looking for a sensible approach, not exhaustive coverage.

## Task 4: Airflow Orchestration

Open `3-airflow/dags/pipeline.py` and complete the DAG.

Implement:

- `run_base`, `run_intermediate`, and `run_marts` using `subprocess.run()` and appropriate `dbt run --select ...` commands
- `report_data` so it reads from `analytics.marts_courses__engagement` and writes `output/reports/course_engagement_{date}.csv`
- use Airflow context variables for date templating, for example `context["ds"]`
- task dependencies so the flow is `base -> intermediate -> marts -> check_mart_quality -> export_mart_report`
- any extra task you think is useful for a clean design

Bonus:

- implement `check_mart_quality`
- validate that the mart has at least one row
- validate that key columns are not null

Focus on structure, clear task design, and reasonable error handling. Simple subprocess-based dbt execution is fine.

## Helpful References

- `SETUP.md`
- `notebooks/example_data_exploration.ipynb`
- [dbt docs](https://docs.getdbt.com/)
- [Airflow TaskFlow API docs](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
