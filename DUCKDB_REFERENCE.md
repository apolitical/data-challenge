# DuckDB reference

DuckDB is broadly PostgreSQL-compatible. This covers the parts this project uses that you
may not have met. Open the warehouse read-only, so nothing you run by hand can change it,
and close the connection before you run dbt (see the lock note in `SETUP.md`):

```bash
uv run python -c "import duckdb; c = duckdb.connect('mock_data.duckdb', read_only=True); print(c.sql('select schema_name, table_name from duckdb_tables() order by 1, 2')); c.close()"
```

`raw` is what the pipeline ingests; `analytics` is what dbt builds.

```sql
-- Lists: a first-class type, written INTEGER[], VARCHAR[] and so on
select [1, 2, 3] as xs;              -- a list literal
select len([1, 2, 3]);               -- 3
select list_contains([1, 2, 3], 2);  -- true
select unnest([1, 2, 3]);            -- one row per element

-- Aggregates you may not know
select arg_max(value, ordering) ...  -- the value from the row with the largest ordering
select count(*) filter (where cond)  -- a conditional count, without a case expression

-- Dates
select cast('2026-09-25' as date) - 7;   -- integer arithmetic on dates gives days
```

Lists arrive from CSV as the bracketed literal `[1,2,3]`, provided the column is declared
with a list type, as `scripts/init_db.py` does. Left to inference, the column would be
`VARCHAR` and the text would not be parsed as a list.
