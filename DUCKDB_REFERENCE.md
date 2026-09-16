# DuckDB reference

A short crib for the parts of DuckDB this project uses that you may not have met before.
DuckDB is broadly PostgreSQL-compatible; this covers the rest.

## Connecting

```bash
uv run python -c "import duckdb; c = duckdb.connect('mock_data.duckdb', read_only=True); print(c.execute('show tables').fetchall())"
```

Read-only is worth using for exploration: DuckDB allows **one writer at a time**, so an
open write connection will make `dbt` fail to take the lock.

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
cast('2026-09-11' as date)
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
uv run dbt build --vars '{"run_date": "2026-09-11"}'
```
