#!/usr/bin/env python3
"""Run the CRM sync once, without Airflow.

Does exactly what the DAG's `push_to_crm` task does: read the engagement mart and push
every member's tier to the CRM. It exists so that the sync can be exercised, and any
change to it tested, without booting the scheduler.

The CRM it writes to comes from the environment, the same way the DAG gets it:

    CRM_BASE_URL   the CRM to write to
    CRM_API_TOKEN  the API token to authenticate with

So run it the way Airflow would:

    uv run --env-file .env python scripts/run_sync.py

Start the CRM environment that `CRM_BASE_URL` points at first, or the sync has nothing
to write to.

This overwrites the CRM's current state, including what it recorded during the incident.
`scripts/reset_state.py` restores it.
"""

import os
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).parent.parent
DB_FILE = ROOT / "mock_data.duckdb"
sys.path.insert(0, str(ROOT / "airflow" / "dags" / "sync_member_engagement"))


def main():
    if "CRM_BASE_URL" not in os.environ:
        print("CRM_BASE_URL is not set, so there is no CRM to write to.")
        print("\nRun this the way Airflow does, with the environment file:")
        print("    cp default.env .env      # then set the absolute paths in it")
        print("    uv run --env-file .env python scripts/run_sync.py")
        return 2

    if not DB_FILE.exists():
        print(f"{DB_FILE.name} not found. Run scripts/init_db.py first.")
        return 2

    from crm_client import sync_engagement_tiers

    conn = duckdb.connect(str(DB_FILE), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT member_id, engagement_tier, previous_tier
            FROM analytics.marts_engagement__member_tiers
            """
        ).fetchall()
    finally:
        conn.close()

    payload = [
        {"member_id": member_id, "engagement_tier": tier, "previous_tier": previous}
        for member_id, tier, previous in rows
    ]

    print(f"syncing {len(payload):,} members to {os.environ['CRM_BASE_URL']}")
    written = sync_engagement_tiers(payload, run_id="manual__run_sync")
    print(f"done: {written:,} members written; task exit status: success")
    return 0


if __name__ == "__main__":
    sys.exit(main())
