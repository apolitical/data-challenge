# Analytics Engineer — Data Challenge

## Overview

This exercise is designed to be completed in **~1-1.5 hours** and assesses your 
ability to:

- Build **rolling active user (RAU) metrics** using window/join patterns
- Construct a **Cohort Retention Matrix** for later analysis
- Structure dbt models across appropriate layers
- Add meaningful tests and documentation

You do **not** need to get everything perfect or fully production-ready.
We are most interested in how you think, how you structure things, and how you 
communicate trade-offs.

## Quick Start

> See [SETUP.md](SETUP.md) for detailed instructions and troubleshooting.

```bash
# 1. Install dependencies
uv sync

# 2. Initialize DuckDB database
uv run python scripts/init_db.py

# 3. Set up dbt
cd dbt_project
uv run dbt deps --profiles-dir .
uv run dbt debug --profiles-dir .

# 4. Run the provided base models
uv run dbt run --select path:models/base --profiles-dir .
uv run dbt test --select path:models/base --profiles-dir .

# 5. (Optional) Explore the data in JupyterLab
cd ..
uv run jupyter lab
```

## What's Provided

### Raw data (`mock_data/`)
| Table | Description | Rows |
|-------|-------------|------|
| `raw_users.csv` | User records (may have duplicates and deleted users) | 80 |
| `raw_courses.csv` | Course catalog | 60 |
| `raw_enrolments.csv` | User course enrolments | 90 |
| `raw_events.csv` | Script generated user interaction events (video, quiz) | 650 |

### Pre-built base models (`models/base/`)

These are **provided and working** — you don't need to modify them:

- `base_raw__users` — Deduplicated, non-deleted users (one row per user)
- `base_raw__events` — Cleaned events with standardized column names
- `base_raw__courses` — Course catalog
- `base_raw__enrolments` — User enrolments
- `base_calendar__dates` — Continuous date spine (one row per calendar date)

### DuckDB SQL reference

See [DUCKDB_REFERENCE.md](DUCKDB_REFERENCE.md) for a BigQuery-to-DuckDB syntax mapping.

---

## Task 1 — Rolling Active Users (Metrics Layer)

Build dbt models that produce a **daily time-series** of active user counts.

### Metrics

- **DAU** — Distinct active users on that date
- **WAU** — Distinct active users in the trailing 7-day window (today + prior 6 days)
- **MAU** — Distinct active users in the trailing 28-day window (today + prior 27 days)

*Example: WAU on 2023-03-15 = distinct users active between 2023-03-09 and 2023-03-15 (7 days inclusive).*

### Requirements

1. One row per calendar date, **no gaps** (use the date spine)
2. Only include non-deleted users
3. You may need an intermediate model
4. Add dbt tests

### Expected output

| window_end_date | dau | wau | mau |
|-----------------|-----|-----|-----|
| 2023-03-03      | 3   | 3   | 3   |
| 2023-03-04      | 0   | 3   | 3   |
| 2023-03-05      | 3   | 6   | 6   |
| ...             | ... | ... | ... |

### Bonus
- Segment RAU by event type (video, quiz)
- Abstract the rolling window logic into a reusable dbt macro

---

## Task 2 — Cohort Retention (Metrics Layer)

Build a **weekly cohort retention** model.

### Definitions

- **Cohort** = Users grouped by signup week: `DATE_TRUNC('week', signup_date)` (Monday of signup week)
- **Week 0** = The calendar week containing signup_date (Mon–Sun)
- **Week N** = N calendar weeks after the cohort week
- **Active** = User has any event in that calendar week
- **Retention rate** = `active_users / cohort_size`

*Example: A user who signs up Thu 2023-04-27 belongs to cohort_week 2023-04-24 (Monday). They are "active in Week 0" if they have any event from Mon 2023-04-24 to Sun 2023-04-30.*

### Requirements

1. One row per `(cohort_week, periods_since)`
2. Include: `cohort_week`, `periods_since`, `cohort_size`, `active_users`, `retention_rate`
3. Add dbt tests

### Expected output

| cohort_week | periods_since | cohort_size | active_users | retention_rate |
|-------------|---------------|-------------|--------------|----------------|
| 2023-03-27  | 0             | 4           | 2            | 0.50           |
| 2023-03-27  | 1             | 4           | 4            | 1.00           |
| 2023-03-27  | 2             | 4           | 1            | 0.25           |

### Bonus
- Add monthly cohort retention as a separate model

---

## Task 3 (Optional) — Dimensional Model (Marts Layer)

Build models in `models/marts/` that are optimised for BI tool consumption (e.g., ThoughtSpot, Looker, Tableau).

Consider how these tools typically expect data to be structured for efficient querying and self-serve analytics. Think about:

- What are the core business entities and how should they be modelled?
- What is the appropriate grain for each model?
- How should fact and dimension tables relate to each other?

### Requirements

1. Each model has a clear, documented grain
2. Relationships between models are explicit and testable
3. Add appropriate dbt tests
4. Add any other good modelling practices

See `models/marts/README.md` for additional hints.

---

## Where to Put Your Models

```
models/
├── base/           ← PROVIDED (don't modify)
├── intermediate/   ← Reusable transforms (if needed)
├── metrics/        ← Tasks 1 & 2: time-series and cohort metrics
└── marts/          ← Task 3 (optional): dimensional model
```

Each directory contains a `README.md` with hints.

## Exploring & Verifying

A Jupyter notebook is provided for data exploration and result verification:

```bash
uv run jupyter lab
```

Open `notebooks/data_exploration.ipynb` — the **"Verify Your Results"** section at the bottom uses pandas on the raw CSV data to spot-check your dbt model output (DAU counts, WAU windows, cohort retention).

## Helpful Resources

- [SETUP.md](SETUP.md) — Setup instructions and useful commands
- [DUCKDB_REFERENCE.md](DUCKDB_REFERENCE.md) — BigQuery → DuckDB syntax reference
- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Testing](https://docs.getdbt.com/docs/build/data-tests)

Good luck, and thank you for taking the time to complete this exercise!
