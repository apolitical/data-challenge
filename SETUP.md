# Setup Guide

## Prerequisites

- Python 3.9+
- [uv](https://docs.astral.sh/uv/) (Python package manager) — Install with: `curl -LsSf https://astral.sh/uv/install.sh | sh`

## Quick Start

### 1. Install dependencies

```bash
uv sync
```

### 2. Initialize the database

```bash
uv run python scripts/init_db.py
```

This creates `mock_data.duckdb` and loads the raw CSV data from `mock_data/`.

### 3. Verify dbt connection

```bash
cd dbt_project
uv run dbt debug --profiles-dir .
```

You should see "All checks passed!".

### 4. Install dbt packages

```bash
uv run dbt deps --profiles-dir .
```

### 5. Run the provided base models

```bash
uv run dbt run --select path:models/base --profiles-dir .
```

### 6. Test the base models

```bash
uv run dbt test --select path:models/base --profiles-dir .
```

You're ready to start building!

### 7. (Optional) Launch JupyterLab

```bash
uv run jupyter lab
```

A notebook is provided at `notebooks/data_exploration.ipynb` for interactive data exploration.

## Useful Commands

```bash
# Run all models
uv run dbt run --profiles-dir .

# Run models in a specific layer
uv run dbt run --select path:models/marts --profiles-dir .
uv run dbt run --select path:models/intermediate --profiles-dir .
uv run dbt run --select path:models/metrics --profiles-dir .

# Run all tests
uv run dbt test --profiles-dir .

# Test a specific model
uv run dbt test --select dim_users --profiles-dir .

# Explore data interactively
uv run python -c "import duckdb; conn = duckdb.connect('mock_data.duckdb'); print(conn.sql('SELECT * FROM raw.users LIMIT 5'))"
```

## Project Structure

```
dbt_project/
├── dbt_project.yml       # Project configuration
├── profiles.yml          # Database connection
├── packages.yml          # dbt package dependencies
├── models/
│   ├── sources.yml       # Raw data source definitions
│   ├── base/             # Provided — cleaned raw data models
│   ├── intermediate/     # Reusable transforms (if needed)
│   ├── metrics/          # Tasks 1 & 2 — build metric models here
│   └── marts/            # Task 3 (optional) — build your star schema here
└── tests/                # Custom data tests
```

## Verifying Your Results

After building your models, open the notebook to spot-check your output:

```bash
uv run jupyter lab
```

The **"Verify Your Results"** section at the bottom of `notebooks/data_exploration.ipynb` uses pandas on the raw CSV data to compute expected values (e.g. DAU per date, cohort sizes), then compares them against your dbt models in the `analytics` schema.

## Troubleshooting

- **"dbt_utils is undefined"** — Run `uv run dbt deps --profiles-dir .` to install packages
- **Database not found** — Run `uv run python scripts/init_db.py` from the project root
- **Model not found** — Make sure you're running dbt commands from `dbt_project/` directory
