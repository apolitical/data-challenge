#!/usr/bin/env python3
"""Start the mock CRM.

It stands in for the production CRM the pipeline writes to: a local mock with dummy
credentials, so nothing in the pipeline can reach a real service. It listens on port
8020 and keeps its state in mock_crm/state/production.json.

scripts/run_pipeline.py and scripts/run_incident.py start one themselves, so you only
need this to look at the API by hand:

Run: uv run python -m mock_crm.run
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

import requests

from mock_crm.service import CrmStore, create_app

ENVIRONMENT = "production"
PORT = 8020
STATE_DIR = Path(__file__).parent / "state"
STATE_FILE = STATE_DIR / f"{ENVIRONMENT}.json"
ROOT = Path(__file__).parent.parent


def is_listening(port):
    """Whether something already answers on the port."""
    try:
        requests.get(f"http://127.0.0.1:{port}/health", timeout=1)
    except requests.RequestException:
        return False
    return True


def start_in_background(port=PORT):
    """Start the mock CRM as a child process and wait until it answers.

    Returns the process; the caller stops it with terminate().
    """
    process = subprocess.Popen(
        [sys.executable, "-m", "mock_crm.run", "--port", str(port)],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(50):
        if process.poll() is not None:
            raise RuntimeError(f"the mock CRM exited immediately with status {process.returncode}")
        if is_listening(port):
            return process
        time.sleep(0.2)
    process.terminate()
    raise RuntimeError(f"the mock CRM did not start on port {port}")


def main():
    parser = argparse.ArgumentParser(description="Start the mock CRM.")
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--rate-limit", type=int, default=None)
    args = parser.parse_args()

    store = CrmStore(STATE_FILE, rate_limit=args.rate_limit)
    print(f"mock CRM [{ENVIRONMENT}] on http://127.0.0.1:{args.port}")
    print(f"   state      {store.path}")
    print(f"   rate limit {store.rate_limit} requests")
    create_app(store).run(host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
