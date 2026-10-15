# Engagement feed migration manifest

Status of the migration from the legacy engagement platform to `engagement_v2`.
Maintained by the platform team. Last updated 2026-09-10.

## Which source system a member is on

`raw.members.source_system` records which engagement platform a member's activity comes
from. A member registered on both platforms has a row for each, so the table's grain is
member × source system.

| Source system | Members | Status |
|---|---|---|
| `legacy_feed` | the majority | still on the legacy platform |
| `engagement_v2` | migrated cohorts | on the new platform |

## What each feed populates

| Column | `legacy_feed` | `engagement_v2` |
|---|---|---|
| `recent_event_ids` | **never populated.** The legacy platform does not emit event identifiers. | populated with the member's event identifiers for the recent window |
| `legacy_event_count` | **maintained.** The legacy platform's own running count of the member's activity, recalculated and re-sent every run. Coarse -- it moves slowly and is not a recent-activity measure -- but current. | **frozen at migration.** Not updated after a member moves to `engagement_v2`, so it is a historical figure from before the move. It does not decrease when a member stops being active, and it says nothing about whether they are active now. |

**The consequence, stated plainly because it has caused confusion before:** for a member
on `engagement_v2`, `legacy_event_count` is a stale historical number. It is not a
measure of recent activity and must not be treated as one.

## What an empty `recent_event_ids` means

This changed on **2026-09-11**, and **only for `engagement_v2`** — migration step 4,
normalising empty collections. The legacy feed is unaffected: it has never populated
this column and still sends nothing for it, so a null from a `legacy_feed` member means
what it always did.

For a member on `engagement_v2`:

| Value | Before 2026-09-11 | From 2026-09-11 |
|---|---|---|
| column absent / null | the member had no recent events | the v2 feed no longer sends this |
| empty list | not sent | the member had no recent events **or** the column is not populated for their source path |

**So from 2026-09-11 an empty list from `engagement_v2` is ambiguous.** Step 4 was
applied to the delivery format before step 5, which restores population for the
remaining source paths; step 5 is not yet scheduled. Until it lands, an empty list from
that feed cannot be read as "this member had no activity" on its own.

`recent_event_ids` is a **window**, not a history: it contains the member's events from
the last seven days, so the same member's list differs between snapshot dates.

The authoritative record of member activity is `raw.events`, which is unaffected by the
migration.
