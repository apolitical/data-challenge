# DuckDB reference

A short crib for the parts of DuckDB this project uses that you may not have met before.
DuckDB is broadly PostgreSQL-compatible; this covers the rest.

## Connecting

```bash
uv run python -c "import duckdb; c = duckdb.connect('mock_data.duckdb', read_only=True); print(c.execute('show tables').fetchall())"
```

Read-only is worth using for exploration: it means nothing you run by hand can change
the warehouse.

`show tables` lists the current schema only. To see everything the warehouse holds,
including which schema each table is in, use DuckDB's catalog function — it also works
when the database is attached under a name rather than opened directly:

```sql
select schema_name, table_name from duckdb_tables() order by 1, 2;
```

It does **not** let you explore while `dbt` is building. DuckDB hands the database file
to one process at a time, and a reader blocks a writer just as a writer does, so an open
connection of either kind makes `dbt` fail with `Could not set lock on file`. That error
reads like the project is broken; usually it means something is still open in another
window. Close it and build again.

## A browser UI

For anything more than a one-liner, DuckDB's own query UI is easier than composing
`python -c` each time:

```bash
uv run python scripts/explore.py
```

It opens a browser, attaches the warehouse read-only and lets you browse the schemas and
run SQL. Tables carry the database name in front:

```sql
select * from mock_data.analytics.marts_engagement__member_tiers limit 10;
```

The UI downloads a DuckDB extension (~30MB) the first time, and fetches its front-end
from DuckDB's own site on every load, so it needs a network connection each time you use
it. Offline, the browser shows `Could not fetch: '/' from 'https://ui.duckdb.org'` -- use
the command line above instead. Your data never leaves the machine; every query runs
locally against your file. Stop the UI before you run `dbt build`.

## Lists

DuckDB has a first-class `LIST` type, written `INTEGER[]`, `VARCHAR[]` and so on.

```sql
select [1, 2, 3] as xs;              -- a list literal
select len([1, 2, 3]);               -- 3
select list_contains([1, 2, 3], 2);  -- true
select unnest([1, 2, 3]);            -- one row per element
select list(x) from t;               -- aggregate rows back into a list
```

Lists arrive from CSV as the bracketed literal `[1,2,3]`, provided the target column is
declared with a list type. If the type is left to inference the column becomes `VARCHAR`
and the text is *not* parsed as a list.

## Aggregates you may not know

```sql
arg_max(value, ordering)              -- the value from the row with the largest ordering
count(*) filter (where condition)     -- a conditional count, without a case expression
```

## Dates

```sql
cast('2026-09-25' as date)
current_date - 7                      -- integer arithmetic on dates gives days
date_trunc('week', event_timestamp)
```

## dbt

Run dbt from `2-dbt_project/` with the profile directory set:

```bash
cd 2-dbt_project
export DBT_PROFILES_DIR=$(pwd)
uv run dbt build
```

The project takes a `run_date` variable, which every date-dependent expression reads
instead of the current date:

```bash
uv run dbt build --vars '{"run_date": "2026-09-25"}'
```
