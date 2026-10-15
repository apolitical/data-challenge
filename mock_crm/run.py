#!/usr/bin/env python3
"""Start a mock CRM environment.

Two environments exist, both local mocks with dummy credentials. Nothing in the
pipeline can reach a real service.

    development  port 8010, state in mock_crm/state/development.json
    production   port 8020, state in mock_crm/state/production.json

Run: uv run python -m mock_crm.run --environment development
"""

import argparse
from pathlib import Path

from mock_crm.service import CrmStore, create_app

ENVIRONMENTS = {"development": 8010, "production": 8020}
STATE_DIR = Path(__file__).parent / "state"


def main():
    parser = argparse.ArgumentParser(description="Start a mock CRM environment.")
    parser.add_argument("--environment", choices=sorted(ENVIRONMENTS), default="development")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--rate-limit", type=int, default=None)
    args = parser.parse_args()

    port = args.port or ENVIRONMENTS[args.environment]
    store = CrmStore(STATE_DIR / f"{args.environment}.json", rate_limit=args.rate_limit)
    print(f"mock CRM [{args.environment}] on http://127.0.0.1:{port}")
    print(f"   state      {store.path}")
    print(f"   rate limit {store.rate_limit} requests")
    create_app(store).run(host="127.0.0.1", port=port)


if __name__ == "__main__":
    main()
