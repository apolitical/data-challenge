# Setup

This project uses `uv` and DuckDB only. You do not need Docker, PostgreSQL, or any other services.

## Prerequisite

Install `uv`:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

If you already have another Python virtualenv active (for example via `pyenv` or `conda`), `uv` will print a warning that `VIRTUAL_ENV` does not match the project's `.venv`. It is safe to ignore. To silence it, run `deactivate` (or `conda deactivate`) before running `uv` commands.

## Quick Start

From the repository root:

```bash
uv sync
uv run python scripts/init_db.py
```

This will:

- install Python dependencies
- create `mock_data.duckdb` in the project root
- create the `raw` schema and tables
- load the CSV files from `mock_data/`
- create indexes
- verify the row counts

You should see row counts for:

- `raw.users`
- `raw.courses`
- `raw.enrolments`
- `raw.events`

## Optional: Jupyter

If you want to explore the data interactively:

```bash
uv run jupyter lab
```

Or:

```bash
uv run jupyter notebook
```

Jupyter usually starts on `http://localhost:8888`.

The example notebook is `notebooks/example_data_exploration.ipynb`. It shows:

- how to connect to DuckDB
- how to inspect the raw tables
- example engagement queries
- a few data quality checks

The notebook connects to DuckDB in read-only mode, so it should not block dbt commands.

## Optional: dbt

Run dbt commands from `2-dbt_project/`:

```bash
cd 2-dbt_project
export DBT_PROFILES_DIR=$(pwd)
```

Useful commands:

```bash
# Check the DuckDB connection
uv run dbt debug

# Compile without running
uv run dbt compile

# Run everything
uv run dbt run

# Run by layer
uv run dbt run --select path:models/base
uv run dbt run --select path:models/intermediate
uv run dbt run --select path:models/marts

# Run a specific model
uv run dbt run --select base_users__users

# Run tests
uv run dbt test
uv run dbt test --select source:raw

# Inspect the graph
uv run dbt ls
uv run dbt ls --select source:*

# Generate docs
uv run dbt docs generate
uv run dbt docs serve
```

Note: the repository uses `models/base/` as the first dbt layer.
If you prefer to run dbt from the repository root, set `DBT_PROFILES_DIR=$(pwd)/2-dbt_project` instead.
You should not normally see a DuckDB lock error from the example notebook.
If you do see one, it usually means another local process has opened `mock_data.duckdb` in write mode. Close that process, then rerun the dbt command.

To identify the process holding the lock:

```bash
lsof mock_data.duckdb
```

To stop that process:

```bash
kill <PID>
```

If it does not stop cleanly:

```bash
kill -9 <PID>
```

## Optional: Airflow

If you want to run the DAG locally:

1. Create a local env file:

```bash
cp default.env .env
```

2. Edit `.env` and set `AIRFLOW_HOME` to the absolute path of this repository's `3-airflow` directory.
Also set `PYTHONPATH` to the absolute path of the `3-airflow` directory.

Example:

```bash
AIRFLOW_HOME=/absolute/path/to/data-challenge/3-airflow
PYTHONPATH=/absolute/path/to/data-challenge/3-airflow
```

3. Start Airflow:

```bash
uv run --env-file .env airflow standalone
```

On macOS, the repository includes a small Airflow workaround:

- `3-airflow/sitecustomize.py`: auto-loaded by Python at startup
- `3-airflow/airflow_macos_standalone_workaround.py`: the actual macOS-specific patch

This avoids a known Airflow standalone crash in forked log-server processes.
For that auto-load to work, `PYTHONPATH` must include `3-airflow/`.

This will:

- initialize the Airflow metadata database
- create an admin user
- start the webserver on port `8080`
- start the scheduler

The login username is `admin`. Airflow writes the password to `standalone_admin_password.txt` and also prints it in the terminal output.

Then:

1. Open `http://localhost:8080`
2. Sign in with the admin credentials
3. Enable the DAG
4. Trigger a run

Stop Airflow with `Ctrl+C`.

If you restart Airflow and see `Address already in use`, a previous Airflow process may still be shutting down or holding a port open.
Check the common ports with:

```bash
lsof -i :8080 -i :8793 -i :8794
```

Then stop the relevant process with:

```bash
kill <PID>
```

## Airflow Command Reference

These commands assume you have already created `.env` and set `AIRFLOW_HOME` correctly.

```bash
# Start Airflow
uv run --env-file .env airflow standalone

# List DAGs
uv run --env-file .env airflow dags list

# Show the DAG structure
uv run --env-file .env airflow dags show apolitical_data_challenge

# Trigger a DAG run
uv run --env-file .env airflow dags trigger apolitical_data_challenge

# View DAG runs
uv run --env-file .env airflow dags list-runs -d apolitical_data_challenge

# Test a specific task without running the full DAG
uv run --env-file .env airflow tasks test apolitical_data_challenge run_base 2024-01-01
```

If you rename DAG or task ids while completing the exercise, update the example commands accordingly.
