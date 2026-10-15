# Setup

## Getting this

```bash
git clone -b senior-data-engineer https://github.com/apolitical/data-challenge.git
cd data-challenge
```

The branch is self-contained: everything the exercise needs is in it, and you do not
need any other branch.

## The order to do things in

Each step below assumes the ones above it.

| | Step | Command | Takes |
|---|---|---|---|
| 1 | install dependencies | `uv sync` | seconds to a minute |
| 2 | load the fixtures into DuckDB | `uv run python scripts/init_db.py` | ~7s |
| 3 | build the models | `cd 2-dbt_project && export DBT_PROFILES_DIR=$(pwd) && uv run dbt build` | ~5s |
| 4 | create your environment file | `cp default.env .env` | — |
| 5 | start a mock CRM | `uv run python -m mock_crm.run --environment production` | leave it running |
| 6 | run the sync into it | `uv run --env-file .env python scripts/run_sync.py` | ~20s |

That is everything needed to run the pipeline. Airflow is **optional** — step 6 exercises
the same sync task the DAG runs. If you do want the DAG, see *Running the DAG*; you only
need to edit `.env` for that.

## Supported environment

Tested on **macOS (arm64) and Linux (x86_64)** with **Python 3.11 or 3.12**. Python 3.13
is not supported: Airflow 2.11 does not support it. The lockfile is committed, so
`uv sync` reproduces the tested dependency set exactly.

`uv` is the only prerequisite:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Quick start

From the repository root:

```bash
uv sync
uv run python scripts/init_db.py
```

That creates `mock_data.duckdb` and loads the fixtures from `mock_data/`. You should see
four tables loaded: `raw.members`, `raw.events`, `raw.member_activity` and
`raw.crm_member_state`.

Then build the models. dbt commands run from `2-dbt_project/`; everything else in
this file runs from the repository root, so come back afterwards:

```bash
cd 2-dbt_project
export DBT_PROFILES_DIR=$(pwd)
uv run dbt build
cd ..
```

The project takes a `run_date` variable, and every date-dependent expression reads it
instead of the current date. It defaults to today, so to build the models as they were
on any past run date:

```bash
uv run dbt build --vars '{"run_date": "2026-09-11"}'
```

**What you should see**, so you can tell our problems from your own. These are for a
build at the **default** run date; a build at a past run date is a different question and
a test may legitimately fail there — that is the exercise, not a broken repository.

| Command | Expected result |
|---|---|
| `uv run python scripts/init_db.py` | four tables loaded, no warnings |
| `uv run dbt run` | all models build, no errors |
| `uv run dbt build` | all models build and **all tests pass** |
| `uv run dbt test --select tag:critical` | passes |
| `uv run pytest mock_crm` | 15 tests pass |

A test failing at a past run date is not automatically a mistake -- that day's data may
genuinely be abnormal, which is the sort of thing worth understanding rather than
silencing.

dbt prints a deprecation summary of its own on most runs. That is dbt talking about dbt;
it is not about this project.

A green run is the expected result, and it is not evidence that anything is fine — the
incident in the brief happened on a run that reported success, and so did the three
runs after it.

If you see anything else — an import error, a missing file, a DuckDB lock error — that
is our problem, not the exercise. Tell us and we will fix it; it will not count against
you.

**One DuckDB gotcha worth knowing up front:** the database file takes a single writer,
and a reader from another process blocks that writer. So a Python session you left
connected — read-only or not — will make `dbt` fail to take the lock. Close the
connection when you are done with it:

```python
conn = duckdb.connect('mock_data.duckdb', read_only=True)
...
conn.close()
```

Read-only is still worth using: it guarantees your exploration cannot change anything.
It just does not remove the need to disconnect.

## Running the mock CRM

Two environments, both local mocks with dummy credentials:

```bash
uv run python -m mock_crm.run --environment development   # port 8010
uv run python -m mock_crm.run --environment production    # port 8020
```

To check one is up, and which environment it is, open its address or `curl` it --
`http://127.0.0.1:8020/` lists the routes it serves and the state file it is writing to,
and `/health` is a bare liveness check. Everything else is under `/v1`.

The API is documented in [`mock_crm/API.md`](mock_crm/API.md) — read that before writing
against it, particularly the sections on idempotency, the rate limit and what triggers
marketing automation.

Each environment keeps its state in its own file (`mock_crm/state/<environment>.json`),
and that file is chosen by `--environment`, **not** by the port. So run **at most one
instance of a given environment at a time**: two processes started with
`--environment production` share `production.json` and will overwrite each other's
writes, even on different ports. If you need a second CRM alongside the first, use the
other environment rather than a second port.

If a port is already in use:

```bash
lsof -i :8010 -i :8020
kill <PID>
```

`--port <n>` overrides the port if something outside this exercise already holds 8010 or
8020. Remember to point `CRM_BASE_URL` in your `.env` at whatever port you chose.

## Running the sync

The sync reads the engagement mart and writes every member's tier to the CRM. To run it
once without Airflow — start the CRM environment it points at first:

```bash
uv run --env-file .env python scripts/run_sync.py
```

It takes the CRM's address from the environment, the same way the DAG's task does, so it
needs a `.env` — `cp default.env .env` is enough, and the copy works unedited for the
sync. The absolute paths in that file matter only if you go on to run Airflow.

## Replaying the incident, and putting it back

```bash
uv run python scripts/run_incident.py    # replay 2026-09-11 to 2026-09-14
uv run python scripts/reset_state.py     # back to the pre-incident starting state
```

`run_incident.py` takes about a minute. It rebuilds the models for each run date and
pushes tiers to the production mock, so afterwards the CRM's state, its recorded tier
transitions and the emails its automation sent are all there to inspect.

`reset_state.py` restores the CRM's state and the dated snapshots from an immutable
copy that ships in `.delivered/`, then reloads the warehouse from the committed
fixtures. It does not run your models or your code to do it, so it works even after a
change that stops the pipeline running. **Run it freely** — you cannot put this
repository into a state you can't get out of.

## Running the DAG

Airflow is optional — `scripts/run_sync.py` exercises the sync without it. If you want
the DAG itself:

```bash
cp default.env .env
```

Edit `.env` and set `AIRFLOW_HOME` and `PYTHONPATH` to the absolute path of this
repository's `3-airflow` directory. Then:

```bash
uv run --env-file .env airflow standalone
```

This initialises the Airflow metadata database, creates an admin user, and starts the
webserver on port `8080` and the scheduler. The username is `admin`; the password is
written to `standalone_admin_password.txt` and printed in the terminal.

Open `http://localhost:8080`, enable `sync-member-engagement`, and trigger a run. The
sync task writes to whichever CRM `CRM_BASE_URL` points at, so **start that environment
first** or the task will fail to connect.

Stop Airflow with `Ctrl+C`. If you restart and see `Address already in use`:

```bash
lsof -i :8080 -i :8793 -i :8794
kill <PID>
```

On macOS this repository includes a workaround for a known Airflow standalone crash in
forked log-server processes:

- `3-airflow/sitecustomize.py` — auto-loaded by Python at startup
- `3-airflow/airflow_macos_standalone_workaround.py` — the patch itself

For the auto-load to work, `PYTHONPATH` must include `3-airflow/`.

## Looking at the data

DuckDB ships a query UI. This starts it against the warehouse:

```bash
uv run python scripts/explore.py
```

It opens a browser, and you can browse both schemas and run SQL there -- `raw` is what
the pipeline ingests, `analytics` is what dbt builds. The warehouse is attached
read-only, so nothing you run here can change it, and tables carry the database name in
front:

```sql
select engagement_tier, count(*)
from mock_data.analytics.marts_engagement__member_tiers
group by 1 order by 2 desc;
```

Three things to know:

- **Stop it before `dbt build`.** DuckDB gives the database file to one process at a
  time, so leaving it open makes dbt fail with `Could not set lock on file`.
- **It needs a network connection, every time.** The first run downloads a DuckDB
  extension (~30MB), and the UI fetches its front-end from DuckDB's own site on every
  load. Offline you get this, in the browser:

  ```
  Could not fetch: '/' from 'https://ui.duckdb.org': Could not establish connection
  ```

  That is the UI, not the exercise. Query from the command line instead --
  [`DUCKDB_REFERENCE.md`](DUCKDB_REFERENCE.md) shows how.

  This is the one thing in the repository that talks to the internet, and it is a
  convenience rather than part of the exercise: every query still runs locally against
  your file, and no data leaves your machine. Nothing in the pipeline itself reaches
  anything remote.
- `--port 4300` if something already holds the default port, `--no-browser` to start
  the server without opening one.

If you would rather not use it, everything is reachable from the command line -- see
[`DUCKDB_REFERENCE.md`](DUCKDB_REFERENCE.md).

## Useful commands

```bash
# what the warehouse holds
uv run python -c "import duckdb; print(duckdb.connect('mock_data.duckdb', read_only=True).execute('show all tables').fetchall())"

# the DAG, without the UI. Both need `airflow standalone` to have been run once --
# that is what creates the Airflow metadata database; without it you get
# "You need to initialize the database". `dags test` runs the real sync, so start the
# CRM environment your .env points at first.
uv run --env-file .env airflow dags list
uv run --env-file .env airflow dags test sync-member-engagement 2026-09-15

# the mock CRM's tests
uv run pytest mock_crm -q
```
