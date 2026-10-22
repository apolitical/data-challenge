#!/usr/bin/env python3
"""Run the pipeline once, as the sync-member-engagement DAG does, without Airflow.

The DAG's three tasks, in order, for one run date:

    build-models        dbt run
    run-critical-tests  dbt test --select tag:critical   a failure stops the run here
    push-to-crm         read the engagement mart and write every member's tier to the CRM

It writes to the production mock CRM on port 8020. If nothing is listening there it starts
that CRM itself and stops it again afterwards; if one is already running, it uses that.

Run: uv run python scripts/run_pipeline.py                         # today, the default
     uv run python scripts/run_pipeline.py --run-date 2026-09-25

The push overwrites the CRM's current state, including what it recorded during the
incident. `scripts/reset_state.py` restores it.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import requests

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "airflow-project"))
sys.path.insert(0, str(ROOT / "airflow-project" / "dags" / "sync_member_engagement"))

import pipeline_config as C  # noqa: E402
from crm_client import CrmClient, sync_engagement_tiers  # noqa: E402
from mock_crm.run import PORT, is_listening, start_in_background  # noqa: E402
from utilities.common import get_dag_configs  # noqa: E402

# The DAG's own configuration, so this runs the same selector against the same model.
CONFIG = get_dag_configs("sync_member_engagement", "production")["common"]
DBT_DIR = ROOT / CONFIG["dbt_project_dir"]
DB_FILE = ROOT / CONFIG["warehouse_file"]


def dbt(command, run_date):
    """Run a dbt command for the run date, as the DAG's BashOperators do."""
    return subprocess.run(
        ["uv", "run", "dbt", *command, "--vars", json.dumps({"run_date": run_date})],
        cwd=DBT_DIR,
        env={**os.environ, "DBT_PROFILES_DIR": str(DBT_DIR)},
    ).returncode


def push_to_crm(run_date, port):
    """Read the engagement mart and write every member's tier to the CRM."""
    base_url = f"http://127.0.0.1:{port}"
    crm = None
    if is_listening(port):
        print(f"   using the CRM already running on {base_url}")
    else:
        crm = start_in_background(port)
        print(f"   started the production mock CRM on {base_url}")

    try:
        conn = duckdb.connect(str(DB_FILE), read_only=True)
        try:
            rows = conn.execute(
                f"""
                SELECT
                    member_id,
                    engagement_tier,
                    previous_tier
                  FROM
                    analytics.{CONFIG['engagement_model']}
                """
            ).fetchall()
        finally:
            conn.close()

        payload = [
            {"member_id": member_id, "engagement_tier": tier, "previous_tier": previous}
            for member_id, tier, previous in rows
        ]
        client = CrmClient(base_url=base_url, api_token=os.environ.get("CRM_API_TOKEN", "mock-token"))
        emails_before = emails_sent(base_url)
        written = sync_engagement_tiers(payload, run_id=f"manual__{run_date}", client=client)
        emails_after = emails_sent(base_url)
        print(
            f"   wrote {written:,} members | {emails_after - emails_before:,} emails sent by "
            f"this run, {emails_after:,} in total | task exit status: success"
        )
    finally:
        if crm is not None:
            crm.terminate()
            crm.wait(timeout=10)


def emails_sent(base_url):
    return len(requests.get(f"{base_url}/v1/automation/emails", timeout=30).json()["emails"])


def main():
    parser = argparse.ArgumentParser(description="Run the pipeline once, without Airflow.")
    parser.add_argument("--run-date", default=C.CURRENT_DATE, help=f"default {C.CURRENT_DATE}")
    parser.add_argument("--port", type=int, default=PORT, help=f"the CRM's port, default {PORT}")
    args = parser.parse_args()
    # dbt writes straight to the terminal; flush each line so the steps print in order.
    sys.stdout.reconfigure(line_buffering=True)

    print(f"[{args.run_date}] build-models")
    if dbt(["run"], args.run_date) != 0:
        print("build-models failed: nothing pushed to the CRM")
        return 1

    print(f"\n[{args.run_date}] run-critical-tests")
    if dbt(["test", "--select", CONFIG["test_selector"]], args.run_date) != 0:
        print("run-critical-tests failed: nothing pushed to the CRM")
        return 1

    print(f"\n[{args.run_date}] push-to-crm")
    push_to_crm(args.run_date, args.port)
    return 0


if __name__ == "__main__":
    sys.exit(main())
