# Setup

```bash
git clone -b senior-data-engineer https://github.com/apolitical/data-challenge.git
cd data-challenge
```

You need [`uv`](https://docs.astral.sh/uv/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`).
Tested on macOS (arm64) and Linux (x86_64) with Python 3.11 or 3.12.

## Getting it running

From the repository root:

```bash
uv sync                                           # install dependencies
uv run python scripts/init_db.py                  # load the fixtures into DuckDB
cd dbt-project && export DBT_PROFILES_DIR=$(pwd)  # dbt commands run from dbt-project/
uv run dbt build                                  # build the models and run every test
cd ..
```

| Command | You should see |
|---|---|
| `init_db.py` | four tables loaded: `raw.members`, `raw.events`, `raw.member_activity`, `raw.crm_member_state` |
| `dbt build` | all models build and all tests pass |
| `uv run pytest mock_crm -q` | 15 tests pass |

A green build is expected, and it isn't evidence that anything is fine: the incident
happened on runs that reported success. If you see anything else (an import error, a
missing file), that's our problem, not the exercise: tell us and it won't count against
you. dbt prints a deprecation summary on most runs; that's dbt talking about dbt.

The models take a `run_date`, which defaults to today. To build them as they stood on a
past run date, from `dbt-project/`:

```bash
uv run dbt build --vars '{"run_date": "2026-09-25"}'
```

A test can legitimately fail at a past run date: that day's data may really be abnormal.

## Running the pipeline

```bash
uv run python scripts/run_pipeline.py                         # today's run, 2026-09-29
uv run python scripts/run_pipeline.py --run-date 2026-09-25   # any run date
```

It does what the DAG does, in the same order and with the DAG's own configuration:
`dbt run`, then `dbt test --select tag:critical`, then the sync, which writes every
member's tier to the CRM. A failed test stops it before the sync. It starts the mock CRM
on port 8020 itself, and stops it afterwards.

**The sync overwrites what the CRM holds**, including its record of the incident. Look
before you run it, or reset afterwards.

## Replaying the incident, and resetting

```bash
uv run python scripts/run_incident.py    # replay 2026-09-25 to 2026-09-28 (3 to 4 minutes)
uv run python scripts/reset_state.py     # back to the state you received
```

The replay rebuilds the models for each run date and pushes tiers to the mock CRM, so
afterwards the CRM's state, its recorded tier transitions and the emails its automation
sent are all there to inspect. The reset restores everything from an immutable copy in
`.delivered/` without running any of your code, so it works even after a change that
breaks the pipeline. **Run it freely.** Both leave the models unbuilt: `dbt build` or
`run_pipeline.py` builds them again.

## Looking at the data and the CRM

From the command line, read-only (`DUCKDB_REFERENCE.md` has more):

```bash
uv run python -c "import duckdb; c = duckdb.connect('mock_data.duckdb', read_only=True); print(c.sql('select engagement_tier, count(*) from analytics.marts_engagement__member_tiers group by 1')); c.close()"
```

Or in DuckDB's browser UI, which needs a network connection to load:
`uv run python scripts/explore.py` (tables are prefixed `mock_data.`).

**DuckDB gives the file to one process at a time**, and a reader blocks a writer: an open
Python session or the UI makes `dbt` fail with `Could not set lock on file`. Close them
before you build.

The mock CRM only runs while `run_pipeline.py` or the replay is running. To query it
yourself, for example for the emails its automation has sent (`mock_crm/API.md`), start it
by hand with `uv run python -m mock_crm.run` and open `http://127.0.0.1:8020/`. While it's
running, `run_pipeline.py` writes to it rather than starting its own, and the replay
refuses to start until you stop it (`lsof -i :8020` finds it).

## Appendix: running the DAG under Airflow

Optional: `run_pipeline.py` runs the same three tasks. Airflow 2.11 is an extra, about a
hundred packages on its own:

```bash
uv sync --extra airflow
cp default.env .env      # then set AIRFLOW_HOME and PYTHONPATH to this repo's airflow-project/ directory
uv run python -m mock_crm.run                        # in another terminal: the DAG's sync needs it
uv run --env-file .env airflow standalone            # webserver on http://localhost:8080
```

The username is `admin`; the password is in `airflow-project/standalone_admin_password.txt`.
Enable `sync-member-engagement` and trigger a run. Without the UI, once `standalone` has
run once: `uv run --env-file .env airflow dags test sync-member-engagement 2026-09-29`.

On macOS, `airflow-project/sitecustomize.py` patches a known crash in Airflow standalone's
forked log server; it loads automatically while `PYTHONPATH` includes `airflow-project/`.
If a port is held after a restart: `lsof -i :8080 -i :8793 -i :8794`.
