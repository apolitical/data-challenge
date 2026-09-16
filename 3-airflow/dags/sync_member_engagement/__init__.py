"""Pipeline metadata for sync-member-engagement.

Shared by every DAG file in this folder. No DAG definition code belongs here: it has to
stay importable by all of them.
"""

from pendulum import datetime

from utilities.common import get_runtime

__runtime__ = get_runtime()
__start_date__ = datetime(2026, 9, 1, tz="UTC")
