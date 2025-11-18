# Setup Instructions

This guide shows how to set up the development environment using **uv** only.

## Prerequisites

- [uv](https://github.com/astral-sh/uv) - Install with: `curl -LsSf https://astral.sh/uv/install.sh | sh`

That's it! No Docker, PostgreSQL, or other dependencies needed.

## Quick Start (2 commands!)

### 1. Initialize Database

```bash
# Create DuckDB database and load CSV data
uv run python scripts/init_db.py
```

This single command will:
- Automatically install all Python dependencies (first run only)
- Create a `mock_data.duckdb` file in the project root
- Create `raw` schema and tables
- Load CSV files from `data/` into DuckDB
- Create indexes for better query performance
- Verify data was loaded correctly

Expected output:
```
============================================================
🚀 Initializing Course Engagement Database (DuckDB)
============================================================

✅ Connected to DuckDB at /path/to/mock_data.duckdb

📋 Creating schema and tables...
✅ Schema and tables created
📥 Loading CSV data...
   ✓ Loaded 6 rows into raw.users
   ✓ Loaded 3 rows into raw.courses
   ✓ Loaded 4 rows into raw.enrolments
   ✓ Loaded 6 rows into raw.events
✅ All data loaded
🔍 Creating indexes...
✅ Indexes created

📊 Verifying data...

Row counts:
   raw.users: 6 rows
   raw.courses: 3 rows
   raw.enrolments: 4 rows
   raw.events: 6 rows

============================================================
✨ Database initialization complete!
============================================================
```

### 2. Start Jupyter Notebook

```bash
# Start JupyterLab
uv run jupyter lab

# Or if you prefer the classic notebook interface
uv run jupyter notebook
```

This will start Jupyter on `http://localhost:8888`

### 3. Open the Example Notebook

Navigate to `notebooks/example_data_exploration.ipynb` to see examples of:
- Connecting to DuckDB
- Querying raw data tables
- Exploring course engagement metrics
- Running data quality checks

### 4. (Optional) Run dbt Models

If you want to work with dbt:

```bash
cd 2-dbt_project

# Test the connection
uv run dbt debug --profiles-dir .

# Run all models
uv run dbt run --profiles-dir .

# Run models by folder
uv run dbt run --select path:models/staging --profiles-dir .
uv run dbt run --select path:models/intermediate --profiles-dir .
uv run dbt run --select path:models/marts --profiles-dir .

# Run tests
uv run dbt test --profiles-dir .
```

### 5. (Optional) Run Airflow

If you want to test the Airflow DAG:

```bash
# Set Airflow home and start (one command!)
export AIRFLOW_HOME=$(pwd)/3-airflow
uv run airflow standalone
```

This will:
- Initialize the database
- Create an admin user with random password
- Start the webserver on port 8080
- Start the scheduler

The admin credentials will be displayed in the terminal output. Look for:
```
standalone | Admin user login: admin
standalone | Admin password: <random-password>
```

Then:
1. Open `http://localhost:8080` in your browser
2. Login with the credentials shown in terminal
3. Enable the `course_engagement_pipeline` DAG in the UI
4. Trigger a manual run to test

**Stopping Airflow:**
```bash
# Press Ctrl+C in the terminal
```

## Useful Commands

### dbt Commands

All dbt commands should be run from the `2-dbt_project/` directory:

```bash
cd 2-dbt_project

# Debug connection to DuckDB
uv run dbt debug --profiles-dir .

# Compile models (don't run)
uv run dbt compile --profiles-dir .

# Run all models
uv run dbt run --profiles-dir .

# Run specific models
uv run dbt run --select base_users__users --profiles-dir .
uv run dbt run --select tag:daily --profiles-dir .

# Run tests
uv run dbt test --profiles-dir .
uv run dbt test --select source:raw --profiles-dir .

# Generate documentation
uv run dbt docs generate --profiles-dir .
uv run dbt docs serve --profiles-dir .

# View lineage
uv run dbt ls --profiles-dir .
uv run dbt ls --select source:* --profiles-dir .
```

### Airflow Commands

All Airflow commands require `AIRFLOW_HOME` to be set:

```bash
# Set Airflow home
export AIRFLOW_HOME=$(pwd)/3-airflow

# Start Airflow (all-in-one: webserver + scheduler + db)
uv run --env-file .env airflow standalone

# List DAGs
uv run --env-file .env airflow dags list

# Test a specific task (without running full DAG)
uv run --env-file .env airflow tasks test course_engagement_pipeline run_staging 2024-01-01

# Trigger a DAG run
uv run --env-file .env airflow dags trigger course_engagement_pipeline

# View DAG structure
uv run --env-file .env airflow dags show course_engagement_pipeline

# View DAG runs
uv run --env-file .env airflow dags list-runs -d course_engagement_pipeline
```
