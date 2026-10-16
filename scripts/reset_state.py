#!/usr/bin/env python3
"""Put the repository back to the state you received it in.

Run this to start again after breaking something.

It restores from an immutable copy of the delivered state that ships in `.delivered/`,
and it does not run your models, your SQL or your CRM client to do it. That is the whole
point: a reset that rebuilt the state by re-running the pipeline would fail exactly when
you most need it -- after a change that stops the pipeline running -- and it would have
already deleted the originals by then.

What it restores:

  - the CRM's state, transitions and the emails its automation sent
  - the dated CRM state snapshots the warehouse ingests
  - the warehouse itself, reloaded from the committed fixtures

It does not rebuild the dbt models. Do that afterwards:

    cd dbt && export DBT_PROFILES_DIR=$(pwd) && uv run dbt build

Run: uv run python scripts/reset_state.py
"""

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "mock_data"
STATE_DIR = ROOT / "mock_crm" / "state"
DELIVERED = ROOT / ".delivered"
DBT_TARGET = ROOT / "dbt" / "target"


def main():
    print("Restoring the state you received\n")

    if not DELIVERED.is_dir():
        print(f"{DELIVERED.name}/ is missing, so there is nothing to restore from.")
        print("Re-extract the archive you were sent. That is our problem, not yours --")
        print("please tell us, it will not count against you.")
        return 1

    # The CRM's state. Replaced wholesale, and the write-ahead journal is discarded:
    # replaying a journal on top of a restored snapshot would reapply the very changes
    # this is undoing.
    # Only what we can put back: a state file the candidate created themselves has
    # nothing to restore from, so deleting it here would be a one-way door in a script
    # that promises the opposite.
    restorable = {source.name for source in (DELIVERED / "mock_crm_state").glob("*.json")}
    for stale in STATE_DIR.glob("*"):
        if stale.name in restorable or stale.suffix != ".json":
            stale.unlink()
    restored_state = 0
    for source in sorted((DELIVERED / "mock_crm_state").glob("*.json")):
        shutil.copy(source, STATE_DIR / source.name)
        restored_state += 1
    print(f"   restored {restored_state} mock CRM state file(s)")

    # The dated snapshots the warehouse ingests from the CRM.
    for stale in DATA_DIR.glob("crm_member_state_*.csv"):
        stale.unlink()
    restored_snapshots = 0
    for source in sorted((DELIVERED / "mock_data").glob("*.csv")):
        shutil.copy(source, DATA_DIR / source.name)
        restored_snapshots += 1
    print(f"   restored {restored_snapshots} dated CRM state snapshot(s)")

    reload = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "init_db.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if reload.returncode != 0:
        print("\n   reloading the warehouse failed:")
        print(reload.stdout[-1500:])
        print(reload.stderr[-1500:])
        return reload.returncode
    print("   reloaded the warehouse from the committed fixtures")

    if DBT_TARGET.exists():
        shutil.rmtree(DBT_TARGET)
        print("   cleared the dbt target directory")

    print("\nDone. Rebuild the models when you are ready:")
    print("   cd dbt && export DBT_PROFILES_DIR=$(pwd) && uv run dbt build")
    return 0


if __name__ == "__main__":
    sys.exit(main())
