#!/usr/bin/env python3
"""Open a browser UI for running SQL against the warehouse.

DuckDB ships a query UI as an extension. This starts it against `mock_data.duckdb`
so you can browse the schemas and run SQL without writing a `python -c` one-liner
every time.

The warehouse is attached **read-only**, so nothing you do here can change it.

It is attached rather than opened directly, and that detail is load-bearing. The UI
keeps its own state -- open tabs, query history -- in a catalog called `_duckdb_ui`,
which it creates on the connection when the page loads. On a connection opened
read-only it cannot create it, and the UI fails on startup with

    Binder Error: Catalog "_duckdb_ui" does not exist!

So the session itself is a writable in-memory database, with `mock_data.duckdb`
attached to it read-only. The UI gets somewhere to write; the warehouse does not
move. Tables are addressed with the database name in front:

    select * from mock_data.analytics.marts_engagement__member_tiers limit 10;

Stop it before you run `dbt build`. DuckDB hands the file to one process at a time,
and a reader blocks a writer just as a writer does: leaving this open makes dbt fail
with `Could not set lock on file`, which reads like the project is broken rather than
like something you left running in another window. Read-only buys you safety, and
lets several read-only sessions share -- it does not let dbt in alongside them.

Two things worth knowing before you rely on it:

  - The first run downloads the `ui` extension (~30MB) into `~/.duckdb/`. After that
    it loads instantly.
  - The UI's front-end is served from DuckDB's own CDN, so it wants a network
    connection. Everything it runs, it runs locally against your file -- no data
    leaves the machine -- but the page itself will not load offline.

Nothing here can write to the warehouse, so it cannot put you in a state that
`scripts/reset_state.py` needs to undo. The only way it can cost you time is by
being left open when you next build.

Run: uv run python scripts/explore.py
     uv run python scripts/explore.py --port 4300 --no-browser
"""

import argparse
import socket
import sys
import threading
from pathlib import Path

import duckdb

ROOT = Path(__file__).parent.parent
DB_FILE = ROOT / "mock_data.duckdb"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--port", type=int, default=4213, help="port to serve the UI on (default: 4213)"
    )
    parser.add_argument(
        "--no-browser", action="store_true", help="start the server without opening a browser"
    )
    args = parser.parse_args()

    if not DB_FILE.exists():
        print(f"{DB_FILE.name} is missing. Load the fixtures first:")
        print("   uv run python scripts/init_db.py")
        return 1

    # Writable in-memory session so the UI can create its `_duckdb_ui` state catalog,
    # with the warehouse attached read-only so nothing can reach it.
    # DuckDB reports success even when the port is taken, and then serves nothing.
    # Check first, so a clash says what it is instead of handing over a dead address.
    probe = socket.socket()
    probe.settimeout(1)
    try:
        in_use = probe.connect_ex(("127.0.0.1", args.port)) == 0
    finally:
        probe.close()
    if in_use:
        print(f"Port {args.port} is already in use -- something else is listening there.")
        print(f"   Either stop it, or pick another port: --port {args.port + 1}")
        return 1

    connection = duckdb.connect()

    try:
        connection.execute("INSTALL ui; LOAD ui;")
    except duckdb.Error as error:
        print("Could not load DuckDB's UI extension:")
        print(f"   {error}")
        print("\nIt downloads on first use, so this usually means no network connection.")
        print("You can still query the warehouse from the command line -- see DUCKDB_REFERENCE.md.")
        return 1

    connection.execute(f"ATTACH '{DB_FILE}' AS mock_data (READ_ONLY);")
    connection.execute(f"SET ui_local_port={args.port};")
    start = "start_ui_server" if args.no_browser else "start_ui"
    print(connection.execute(f"CALL {start}()").fetchone()[0])

    print("\nThe warehouse is attached read-only as `mock_data`, so queries name it:")
    print("   select * from mock_data.analytics.marts_engagement__member_tiers limit 10;")
    print("Stop the UI before `dbt build` -- DuckDB gives the file to one process at a time.")
    print("Schemas: `raw` is what the pipeline ingests, `analytics` is what dbt builds.")

    # The UI runs on a background thread, so the server dies the moment this function
    # returns. Block here until the user stops it.
    try:
        input("\nPress Enter or Ctrl-C to stop the UI.\n")
    except KeyboardInterrupt:
        print()
    except EOFError:
        # No terminal attached -- serve until interrupted rather than exiting at once.
        print("\nNo terminal attached. Serving until interrupted.")
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
