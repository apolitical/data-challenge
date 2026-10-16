# Mock CRM API

A local stand-in for the CRM the engagement pipeline writes to. It persists to a JSON
file so state survives between pipeline runs.

It stands in for the **production** CRM, and is a **local mock with dummy credentials**:
`http://127.0.0.1:8020`, state in `mock_crm/state/production.json`.
`scripts/run_pipeline.py` and `scripts/run_incident.py` start it themselves. To start it
by hand:

```bash
uv run python -m mock_crm.run
```

Run **at most one at a time**: two processes share `production.json` and overwrite each
other's writes, even on different ports.

## Is it running?

| Method | Path | Notes |
|---|---|---|
| `GET` | `/` | What this instance is: environment, state file, rate limit, and the routes it serves. |
| `GET` | `/health` | `{"status": "ok"}` while the store is readable. |

Everything else lives under `/v1`, so these two exist to answer "is the server up and
which environment is it". Neither consumes rate-limit quota, so polling them is free.

## Members

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/members?offset=&limit=` | One page of member records, ordered by member id. Page size is capped at **500** however large a `limit` is requested. Response carries `total` and `next_offset`; `next_offset` is `null` on the last page. |
| `GET` | `/v1/members/{id}` | One member record. `404` if the CRM has no record for them. |
| `PATCH` | `/v1/members/{id}` | Merge the posted properties into the member's record. **Idempotent**: sending the same properties again changes nothing. |

## Timeline notes

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/members/{id}/notes` | The member's timeline notes. |
| `POST` | `/v1/members/{id}/notes` | Append a note. **Not idempotent**: posting the same note twice leaves two notes. There is no idempotency key and no endpoint to delete a note. |

## Marketing automation

The CRM runs marketing automation off the engagement tier it holds. The behaviour that
matters to this pipeline:

- The re-engagement campaign fires on the **transition into** the `dormant` tier. It
  reacts to the change, not to the value — writing `dormant` to a member who is already
  `dormant` sends nothing.
- **No automation fires on the transition out of `dormant`.**
- **A correcting write does not un-send an email that has already gone out.** There is
  no endpoint to recall a sent email; the record is append-only.

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/automation/emails` | Emails the automation has sent. |
| `GET` | `/v1/automation/transitions` | Every tier transition the CRM has recorded. |

## Sync runs

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/sync/runs` | Runs the CRM has been told about. |
| `POST` | `/v1/sync/runs` | Record that a sync run completed. Closes the current rate-limit window and opens the next, so it is never itself refused for being over the limit. |

## Rate limit

**30,000 requests per window.** One window per sync run: recording a completed run at
`POST /v1/sync/runs` opens the next window. Reads count against it as well as writes, so a strategy that reads the current state before writing has to be
deliberate about how much it reads: paging the whole population costs about 25 requests,
while reading each member individually costs one per member.

Over the limit the API returns **`429`** with `{"error": "rate_limited"}`.

## Test and admin endpoints

Not part of the CRM's own API. Provided so unhappy paths can be demonstrated rather
than argued about. They do not count against the rate limit.

| Method | Path | Notes |
|---|---|---|
| `POST` | `/v1/admin/baseline` | Record the current state as the point `reset` returns to. |
| `POST` | `/v1/admin/reset` | Discard everything since the baseline: properties, notes, emails and transitions. |
| `POST` | `/v1/admin/compact` | Fold the write journal into the state snapshot, so the snapshot is complete on its own. |
| `POST` | `/v1/admin/new_window` | Start a new rate-limit window, restoring the full quota. |
| `POST` | `/v1/admin/fail_next` | `{"path": "...", "times": n}` — make the next `n` requests to that path fail with a `500`. |
