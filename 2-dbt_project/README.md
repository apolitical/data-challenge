# dbt Project - Course Engagement Analytics

This dbt project transforms raw course engagement data into analytics-ready models.

## Quick Start

### 1. Initialize the database first

From the project root:
```bash
uv run python scripts/init_db.py
```

### 2. Test dbt connection

From this directory (`2-dbt_project/`):
```bash
uv run dbt debug --profiles-dir .
```

You should see:
```
All checks passed!
```

### 3. Run dbt models

```bash
# Run all models
uv run dbt run --profiles-dir .

# Run by layer
uv run dbt run --select path:models/staging --profiles-dir .
uv run dbt run --select path:models/intermediate --profiles-dir .
uv run dbt run --select path:models/marts --profiles-dir .
```

## Project Structure

```
2-dbt_project/
├── dbt_project.yml       # Project configuration
├── profiles.yml          # DuckDB connection settings
└── models/
    ├── sources.yml       # Raw data source definitions
    ├── staging/          # Cleaned, deduped raw data (views)
    ├── intermediate/     # Joined, transformed data (views)
    └── marts/            # Final analytics tables (tables)
```

## Database Configuration

- **Database:** DuckDB (embedded, file-based)
- **Location:** `../mock_data.duckdb`
- **Source schema:** `raw`
- **Target schema:** `analytics` (for dbt models)
- **Adapter:** dbt-duckdb

## Development Workflow

1. **Create staging models** - Clean and standardize raw data
   - Deduplicate users
   - Filter deleted records
   - Normalize column names
   - Cast data types

2. **Create intermediate models** - Join and transform
   - User-course relationships
   - Event aggregations
   - Reusable business logic

3. **Create marts** - Final analytics models
   - Course engagement summary
   - User activity metrics
   - Aggregated KPIs

## Common Commands

```bash
# Generate documentation
uv run dbt docs generate --profiles-dir .
uv run dbt docs serve --profiles-dir .

# Run tests
uv run dbt test --profiles-dir .

# Test sources
uv run dbt test --select source:* --profiles-dir .

# View model lineage
uv run dbt ls --profiles-dir .
```

## Notes

- Always use `--profiles-dir .` to use the local `profiles.yml`
- Source data is in the `raw` schema (loaded by `scripts/init_db.py`)
- All dbt models will be created in the `analytics` schema
- DuckDB doesn't require a running database server
