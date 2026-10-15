#!/usr/bin/env python3
"""Configuration the pipeline and its operational scripts share.

Ships with the repository. Everything here is either visible in the models already or
needs to be, so that the incident replay and the reset are runnable.
"""

# The pipeline runs daily. These are the run dates the committed fixtures cover.
PRE_INCIDENT_DATE = "2026-09-10"   # the last good run
INCIDENT_DATE = "2026-09-11"       # the run whose tiers went wrong
DISCOVERY_DATE = "2026-09-14"      # the day a member complained
CURRENT_DATE = "2026-09-15"        # today: the run you would make yourself

RUN_DATES = [
    "2026-09-10", "2026-09-11", "2026-09-12", "2026-09-13", "2026-09-14", "2026-09-15",
]

# The dates scripts/run_incident.py replays. Today is not replayed.
REPLAYED_DATES = RUN_DATES[1:-1]

# The rolling window the engagement model aggregates over, in days.
ACTIVITY_WINDOW_DAYS = 7

# Source systems, as recorded on the member dimension. Which of them the legacy
# lifetime counter is still maintained for is documented in MIGRATION_MANIFEST.md.
SOURCE_LEGACY = "legacy_feed"
SOURCE_V2 = "engagement_v2"

# Tier thresholds, as the engagement model implements them.
TIER_THRESHOLDS = [(50, "champion"), (20, "engaged"), (5, "at_risk")]
TIER_FLOOR = "dormant"
