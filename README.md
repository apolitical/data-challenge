# Data Challenge

## Overview

This exercise is designed to be completed in **1.5–2 hours** and assesses your ability to:

- Understand and refactor a complex SQL query
- Structure transformations into sensible dbt layers (staging / intermediate / marts)
- Add basic data quality tests and documentation
- Implement an Airflow DAG using the TaskFlow API to orchestrate dbt runs and data exports

You do **not** need to get everything perfect or fully production-ready.
We are most interested in how you think, how you structure things, and how you communicate trade-offs.

---

## Quick Start

This project uses **DuckDB** (embedded database) and **uv** (Python package manager)

### Prerequisites

- [uv](https://github.com/astral-sh/uv) - Install with: `curl -LsSf https://astral.sh/uv/install.sh | sh`

### Setups

> See `SETUP.md` for detailed instructions, DBT commands, jupyter notebook access, and Airflow commands.

#### For Task 1 to 3

```bash
# 1. Initialize DuckDB database and load CSV data
uv run python scripts/init_db.py

# 2. (Optional) Start Jupyter to explore the data
uv run jupyter lab # or jupyter notebook
```

#### For Task 3

```bash
# 1. Set up .env locally for Airflow Home
cp default.env .env
## Edit the variable "AIRFLOW_HOME" to current working directory with absolute path

# 2. Start Airflow in standalone mode
uv run --env-file .env airflow standalone
```

---

## Repository Structure

- `data/` - Sample CSV data files
  - `raw_users.csv`
  - `raw_courses.csv`
  - `raw_enrolments.csv`
  - `raw_events.csv`
- `1-sql/`
  - `messy_course_engagement_duckdb.sql`  ← legacy query to refactor
- `2-dbt_project/`
  - `dbt_project.yml`
  - `profiles.yml` - dbt configuration for DuckDB
  - `models/`
    - `base/` - Create your base/staging models here
    - `intermediate/` - Create intermediate models here
    - `marts/` - Create final mart models here
    - `sources.yml` - Source table definitions
- `3-airflow/dags/`
  - `pipeline.py`  ← Airflow TaskFlow DAG to implement
- `notebooks/`
  - `example_data_exploration.ipynb` - Example queries to help you explore the data
- `scripts/`
  - `init_db.py` - Database initialization script

---

## Scenario

You've joined a small data team. A key stakeholder relies on a legacy query in `1-sql/messy_course_engagement_duckdb.sql` to understand course engagement.

This query:

- Joins users, courses, enrolments, and events
- Computes basic metrics at the **course** level
- Is difficult to maintain and extend
- Has some modelling and data quality issues (intentional anti-patterns!)

Your task is to refactor this into a small dbt project and implement Airflow orchestration.

---

## Task 1 — Understand the Legacy Query

1. Open `1-sql/messy_course_engagement_duckdb.sql`.
2. Understand:
   - What tables are involved? (Hint: `raw.users`, `raw.courses`, `raw.enrolments`, `raw.events`)
   - What is the **intended grain** of the final result? (one row per course)
   - What metrics is it trying to compute?
   - What are the anti-patterns in the query?
3. You can explore the data using the Jupyter notebook: `notebooks/example_data_exploration.ipynb`

**Note:** You do **not** need to give a long explanation, but it should be clear enough to justify your refactor inton DBT models in Task 2. 

---

## Task 2 — Refactor into DBT Models

Using the DBT project skeleton in `2-dbt_project/`:

1. Create **base(staging) models** in `models/base/`, for example:
   - `base_users__users.sql`
   - `base_courses__courses.sql`
   - ...
2. Create **intermediate model(s)** in `models/intermediate/`, for example:
   - `intermediate_events__events.sql`
   - ...
3. Create **a final mart** in `models/marts/`, for example:
   - `marts_courses__engagements.sql`
     - The final mart should be at **one row per course** with metrics such as:
       - `learners` - Distinct users enrolled
       - `active_learners` - Distinct users with events
       - `total_quizzes_completed`
       - `total_videos_completed`
       - `first_activity`
       - `last_activity`

**Notes:**
- Use `ref()` and `source()` appropriately so that dependencies are clear.

---

## Task 3 — Add Basic Tests and Documentation

In `2-dbt_project/models/`:

1. Add a `schema.yml` (or extend existing files) to define:
   - At least **one or two models** with:
     - `unique` and `not_null` tests on suitable keys
     - A `relationships` test where appropriate
2. Add brief **descriptions** for:
   - The final mart model
   - A few important columns

We are not expecting exhaustive coverage, just enough to demonstrate your approach.

---

## Task 4 — Airflow Orchestration (TaskFlow API)

Open `3-airflow/dags/pipeline.py`.

1. **Implement dbt orchestration tasks:**
   - Update `run_base`, `run_intermediate`, and `run_marts` to call `dbt run` with appropriate `--select` flags
   - Use sensible grouping to run the models
   - Use `subprocess.run()` to execute dbt commands
   - Add any other tasks that you think necessary for good practice

2. **Add data export task:**
   - Implement `report_data` to:
     - Query the `analytics.mart_course_engagement` table from DuckDB
     - Export results to `output/reports/course_engagement_{date}.csv`
     - Use Airflow's context variables for date templating (e.g., `context['ds']`)
   - This simulates delivering results to stakeholders

3. **(Bonus) Add quality check task:**
   - Implement `check_mart_quality` to validate:
     - Mart table has at least 1 row
     - No nulls in key columns
     - Raise an error or log warning if checks fail

4. **Ensure correct task dependencies:**
   - `base` → `intermediate` → `marts` → `check_mart_quality` → `export_mart_report`

See `SETUP.md` for detailed Airflow setup instructions.

**Notes:**
- Focus on **structure**, **error handling**, and demonstrating understanding of the TaskFlow API
- See the TODO comments in the DAG file for implementation examples
- Simple subprocess calls are fine for dbt commands

---

## Helpful Resources

- **SETUP.md** - Detailed setup instructions and all commands
- **CLAUDE.md** - Architecture overview and development guide
- **notebooks/example_data_exploration.ipynb** - Example queries to explore the data
- [dbt Documentation](https://docs.getdbt.com/)
- [Airflow TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)

---

Good luck, and thank you for taking the time to complete this exercise!
