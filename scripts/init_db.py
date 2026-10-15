#!/usr/bin/env python3
"""Load the member engagement fixture from mock_data/ into DuckDB.

Every table is created with an explicit column type before loading, and that is
deliberate rather than tidiness. `recent_event_ids` must be stored as a genuine
`INTEGER[]`: if the type is left to CSV inference it becomes `VARCHAR`, `len()` then
silently returns the *string* length instead of the list length, and every engagement
score in the project is quietly wrong in a different way with no error raised anywhere.

Run: uv run python scripts/init_db.py
"""

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).parent.parent
DB_FILE = ROOT / "mock_data.duckdb"
DATA_DIR = ROOT / "mock_data"

# table name -> (DDL, glob of the CSV files that load into it)
TABLES = {
    "members": (
        """
        CREATE TABLE raw.members (
            member_id INTEGER,
            full_name VARCHAR,
            email VARCHAR,
            source_system VARCHAR,
            signup_date DATE,
            country VARCHAR
        );
        """,
        "raw_members.csv",
    ),
    "events": (
        """
        CREATE TABLE raw.events (
            event_id BIGINT,
            member_id INTEGER,
            event_type VARCHAR,
            event_timestamp TIMESTAMP
        );
        """,
        "raw_events.csv",
    ),
    "member_activity": (
        """
        CREATE TABLE raw.member_activity (
            member_id INTEGER,
            snapshot_date DATE,
            recent_event_ids INTEGER[],
            legacy_event_count INTEGER
        );
        """,
        "member_activity_*.csv",
    ),
    "crm_member_state": (
        """
        CREATE TABLE raw.crm_member_state (
            member_id INTEGER,
            state_date DATE,
            engagement_tier VARCHAR,
            is_active BOOLEAN,
            last_synced_at TIMESTAMP
        );
        """,
        "crm_member_state_*.csv",
    ),
}


def load(conn):
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    for table, (ddl, pattern) in TABLES.items():
        files = sorted(DATA_DIR.glob(pattern))
        if not files:
            print(f"   !  no files matching mock_data/{pattern}; skipping raw.{table}")
            continue
        conn.execute(f"DROP TABLE IF EXISTS raw.{table};")
        conn.execute(ddl)
        for csv_file in files:
            # nullstr='' makes an empty CSV field a NULL. An empty *list* is written
            # as the token [] and stays an empty list, which is what keeps "no recent
            # events" distinguishable from "no value at all".
            conn.execute(
                f"""
                INSERT INTO raw.{table}
                SELECT * FROM read_csv('{csv_file}', nullstr='', header=true,
                                       columns={columns_for(conn, table)});
                """
            )
        count = conn.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
        print(f"   loaded {count:>7,} rows into raw.{table}  ({len(files)} file(s))")


def columns_for(conn, table):
    """Return the column name -> type map for an already-created raw table.

    Passing this to read_csv pins the CSV reader to the declared types instead of
    letting it infer them, which is what keeps the list column a real list.
    """
    rows = conn.execute(
        """
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = ? ORDER BY ordinal_position
        """,
        [table],
    ).fetchall()
    return "{" + ", ".join(f"'{name}': '{dtype}'" for name, dtype in rows) + "}"


def main():
    print(f"Initialising DuckDB at {DB_FILE.name}")
    if not DATA_DIR.exists():
        print(f"❌ mock_data/ not found at {DATA_DIR}")
        return 1
    conn = duckdb.connect(str(DB_FILE))
    try:
        load(conn)
    finally:
        conn.close()
    print("\nDone. Next: cd 2-dbt_project && export DBT_PROFILES_DIR=$(pwd) && uv run dbt build")
    return 0


if __name__ == "__main__":
    sys.exit(main())
