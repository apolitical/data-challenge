#!/usr/bin/env python3
"""
Initialize DuckDB database with course engagement data.

This script:
1. Creates a DuckDB database file
2. Creates the raw schema and tables
3. Loads CSV data from mock_data/
4. Creates indexes for query performance
"""

import sys
from pathlib import Path
import pandas as pd
import duckdb


# Database file location
DB_FILE = Path(__file__).parent.parent / 'mock_data.duckdb'

# Base directory for data files
DATA_DIR = Path(__file__).parent.parent / 'mock_data'


def get_connection():
    """Create database connection."""
    try:
        # Connect to DuckDB (creates file if it doesn't exist)
        conn = duckdb.connect(str(DB_FILE))
        return conn
    except Exception as e:
        print("❌ Error: Could not create/connect to DuckDB database")
        print(f"   Details: {e}")
        sys.exit(1)


def create_schema_and_tables(conn):
    """Create raw schema and tables."""
    print("📋 Creating schema and tables...")

    # Create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    # Drop existing tables if they exist
    conn.execute("DROP TABLE IF EXISTS raw.events;")
    conn.execute("DROP TABLE IF EXISTS raw.enrolments;")
    conn.execute("DROP TABLE IF EXISTS raw.courses;")
    conn.execute("DROP TABLE IF EXISTS raw.users;")

    # Create users table
    conn.execute("""
        CREATE TABLE raw.users (
            id INTEGER,
            fullName VARCHAR,
            email VARCHAR,
            signupDate DATE,
            state VARCHAR,
            isGovEmployee BOOLEAN,
            updatedAt TIMESTAMP,
            deleted BOOLEAN
        );
    """)

    # Create courses table
    conn.execute("""
        CREATE TABLE raw.courses (
            course_id INTEGER,
            title VARCHAR,
            category_name VARCHAR,
            level VARCHAR,
            publisher VARCHAR,
            course_created_at DATE
        );
    """)

    # Create enrolments table
    conn.execute("""
        CREATE TABLE raw.enrolments (
            enrolment_id INTEGER,
            user_id INTEGER,
            course_id INTEGER,
            enrolled_at TIMESTAMP,
            status VARCHAR
        );
    """)

    # Create events table
    conn.execute("""
        CREATE TABLE raw.events (
            id INTEGER,
            user_id INTEGER,
            course_id INTEGER,
            event_type VARCHAR,
            event_timestamp TIMESTAMP,
            session_id VARCHAR,
            metadata VARCHAR
        );
    """)

    print("✅ Schema and tables created")


def load_csv_data(conn):
    """Load CSV data into tables using DuckDB's CSV reader."""
    print("📥 Loading CSV data...")

    # Define CSV files and their target tables
    csv_files = {
        'users': DATA_DIR / 'raw_users.csv',
        'courses': DATA_DIR / 'raw_courses.csv',
        'enrolments': DATA_DIR / 'raw_enrolments.csv',
        'events': DATA_DIR / 'raw_events.csv'
    }

    # Load each table using DuckDB's efficient CSV reader
    for table_name, csv_file in csv_files.items():
        if not csv_file.exists():
            print(f"⚠️  Warning: {csv_file} not found, skipping...")
            continue

        # DuckDB can read CSV directly and handle NULL values properly
        conn.execute(f"""
            INSERT INTO raw.{table_name}
            SELECT * FROM read_csv_auto('{csv_file}',
                nullstr='',
                header=true
            );
        """)

        # Get row count
        result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
        row_count = result[0]

        print(f"   ✓ Loaded {row_count} rows into raw.{table_name}")

    print("✅ All data loaded")


def create_indexes(conn):
    """Create indexes for better query performance."""
    print("🔍 Creating indexes...")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_users_id ON raw.users(id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_enrolments_user_id ON raw.enrolments(user_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_enrolments_course_id ON raw.enrolments(course_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_user_id ON raw.events(user_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_course_id ON raw.events(course_id);")

    print("✅ Indexes created")


def verify_data(conn):
    """Verify data was loaded correctly."""
    print("\n📊 Verifying data...")

    tables = ['users', 'courses', 'enrolments', 'events']

    print("\nRow counts:")
    for table in tables:
        result = conn.execute(f"SELECT COUNT(*) FROM raw.{table};").fetchone()
        count = result[0]
        print(f"   raw.{table}: {count} rows")


def main():
    """Main initialization function."""
    print("=" * 60)
    print("🚀 Initializing Course Engagement Database (DuckDB)")
    print("=" * 60)
    print()

    # Connect to database
    conn = get_connection()
    print(f"✅ Connected to DuckDB at {DB_FILE}\n")

    try:
        # Generate events data (always regenerate for consistency)
        print("🎲 Generating events data...")
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "generate_events",
            Path(__file__).parent / "generate_events.py"
        )
        gen_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gen_mod)
        gen_mod.main()
        print()

        # Run initialization steps
        create_schema_and_tables(conn)
        load_csv_data(conn)
        create_indexes(conn)
        verify_data(conn)

        print("\n" + "=" * 60)
        print("✨ Database initialization complete!")
        print("=" * 60)
        print("\nYou can now:")
        print("  • Start Jupyter: uv run jupyter lab")
        print(f"  • Database file: {DB_FILE}")
        print(f"  • Connect with: duckdb.connect('{DB_FILE}')")
        print()

    except Exception as e:
        print(f"\n❌ Error during initialization: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
