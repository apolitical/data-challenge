#!/usr/bin/env python3
"""Replay the pipeline's run history, including the incident.

This is what makes the incident something the candidate can observe rather than
something the brief asserts. It reproduces, in the CRM's own state:

  2026-09-24  the last good run, seeded from the committed fixture
  2026-09-25  the run where the upstream migration lands and tiers go wrong
  2026-09-26  a run that reports success
  2026-09-27  a run that reports success
  2026-09-28  a run that reports success -- the day a member complains

Each replayed run does what the DAG does: ingest the CRM's current state back into the
warehouse, rebuild the models for that run date, and push every member's tier to the
CRM. The mock CRM records the tier transitions and the marketing emails those
transitions triggered.

Run: uv run python scripts/run_incident.py
Start again from the pre-incident state with: uv run python scripts/reset_state.py
"""

import argparse
import csv
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import requests

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import pipeline_config as C  # noqa: E402
from mock_crm.run import PORT, STATE_FILE, is_listening, start_in_background  # noqa: E402
from mock_crm.service import CrmStore  # noqa: E402

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "mock_data"
DB_FILE = ROOT / "mock_data.duckdb"
DBT_DIR = ROOT / "dbt"

# The replay's own CRM listens on mock_crm.run's PORT (imported above). Rebound by --port,
# in case something outside this exercise already holds the default.
BASE_URL = f"http://127.0.0.1:{PORT}"


def seed_crm_from_fixture():
    """Load the CRM's pre-incident state straight into its store.

    Seeded directly rather than replayed: 2026-09-24 is where the history starts, not
    something this script reproduces.
    """
    store = CrmStore(STATE_FILE)
    store._state = {"members": {}, "emails": [], "transitions": [], "runs": []}
    fixture = DATA_DIR / f"crm_member_state_{C.PRE_INCIDENT_DATE}.csv"
    with fixture.open() as fh:
        for row in csv.DictReader(fh):
            store._state["members"][row["member_id"]] = {
                "properties": {"engagement_tier": row["engagement_tier"]},
                "notes": [],
            }
    store._flush()
    store.save_baseline()
    print(f"   seeded {len(store._state['members']):,} members from {fixture.name}")


def refuse_if_crm_running():
    """Stop before touching anything if a CRM is already listening on the replay's port.

    The replay seeds the CRM's state file and then starts its own CRM on it. A CRM
    already running holds that file too, so the two would overwrite each other's
    writes; and attaching to it would replay the incident into whatever state it
    happens to hold, silently. So check first, before the seed rewrites the file.
    """
    if is_listening(PORT):
        raise SystemExit(
            f"Something is already listening on {BASE_URL}.\n"
            "This script starts its own CRM and needs that port to itself, so that the\n"
            "replay cannot write into a service holding different state. Stop it first:\n"
            f"    lsof -i :{PORT}\n"
            "Or run this script against another port with --port."
        )


def export_crm_state(run_date):
    """Write the CRM's current state to the warehouse's ingest directory.

    This is the step that closes the loop: `is_active` is derived from the tier the
    CRM holds, and the tier the CRM holds was written by the previous run.
    """
    members = {}
    offset = 0
    while offset is not None:
        page = requests.get(
            f"{BASE_URL}/v1/members", params={"offset": offset, "limit": 500}, timeout=30
        ).json()
        members.update(page["members"])
        offset = page["next_offset"]
    path = DATA_DIR / f"crm_member_state_{run_date}.csv"
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ["member_id", "state_date", "engagement_tier", "is_active", "last_synced_at"]
        )
        for member_id, record in sorted(members.items(), key=lambda kv: int(kv[0])):
            tier = record["properties"].get("engagement_tier")
            writer.writerow(
                [
                    member_id,
                    run_date,
                    tier,
                    str(tier != C.TIER_FLOOR).lower(),
                    f"{run_date} 02:15:00",
                ]
            )
    return path


def build_models(run_date):
    """Rebuild the dbt models for one run date, exactly as the DAG's task does."""
    result = subprocess.run(
        ["uv", "run", "dbt", "run", "--quiet", "--vars", f'{{"run_date": "{run_date}"}}'],
        cwd=DBT_DIR,
        env={**__import__("os").environ, "DBT_PROFILES_DIR": str(DBT_DIR)},
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stdout[-2000:])
        print(result.stderr[-2000:])
        raise RuntimeError(f"dbt run failed for {run_date}")


def push_tiers(run_date):
    """Push every member's tier to the CRM, as the DAG's sync task does."""
    sys.path.insert(0, str(ROOT / "airflow" / "dags" / "sync_member_engagement"))
    from crm_client import CrmClient, sync_engagement_tiers

    conn = duckdb.connect(str(DB_FILE), read_only=True)
    try:
        rows = conn.execute(
            "SELECT member_id, engagement_tier, previous_tier FROM analytics.marts_engagement__member_tiers"
        ).fetchall()
    finally:
        conn.close()
    payload = [
        {"member_id": m, "engagement_tier": t, "previous_tier": p} for m, t, p in rows
    ]
    client = CrmClient(base_url=BASE_URL, api_token="mock-token")
    return sync_engagement_tiers(payload, run_id=f"scheduled__{run_date}", client=client)


def reload_warehouse():
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "init_db.py")],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )


def main():
    global PORT, BASE_URL
    parser = argparse.ArgumentParser(description="Replay the incident against a local mock CRM.")
    parser.add_argument(
        "--port",
        type=int,
        default=PORT,
        help=f"port for the replay's own mock CRM (default {PORT})",
    )
    args = parser.parse_args()
    PORT = args.port
    BASE_URL = f"http://127.0.0.1:{PORT}"

    refuse_if_crm_running()

    print("Replaying the pipeline's run history\n")
    print(f"[{C.PRE_INCIDENT_DATE}] last good run")
    seed_crm_from_fixture()

    crm = start_in_background(PORT)
    try:
        for run_date in C.REPLAYED_DATES:
            started = time.time()
            print(f"\n[{run_date}] scheduled run")
            # Each daily run is its own rate-limit window.
            requests.post(f"{BASE_URL}/v1/admin/new_window", timeout=5)
            export_crm_state(run_date)
            reload_warehouse()
            build_models(run_date)
            written = push_tiers(run_date)
            emails = len(requests.get(f"{BASE_URL}/v1/automation/emails", timeout=30).json()["emails"])
            print(
                f"   wrote {written:,} members | {len(requests.get(f'{BASE_URL}/v1/automation/transitions', timeout=30).json()['transitions']):,} "
                f"transitions recorded | {emails:,} emails sent so far | "
                f"{time.time() - started:.0f}s | task exit status: success"
            )
        # Leave the warehouse holding the CRM state as of today, so the candidate's
        # own run reads the state the last replayed run produced.
        export_crm_state(C.CURRENT_DATE)
        reload_warehouse()
        # Fold the write journal into the snapshot. The snapshot is what gets shipped
        # and what reset_state restores, so it has to be complete on its own.
        requests.post(f"{BASE_URL}/v1/admin/compact", timeout=60)
    finally:
        crm.terminate()
        crm.wait(timeout=10)

    print(f"\nHistory replayed to {C.DISCOVERY_DATE}. Every run reported success.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
