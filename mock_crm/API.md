# Mock CRM API

A local stand-in for the production CRM the pipeline writes to, with dummy credentials.
It listens on `http://127.0.0.1:8020` and keeps its state in
`mock_crm/state/production.json`, so state survives between runs. `scripts/run_pipeline.py`
and `scripts/run_incident.py` start it themselves; `uv run python -m mock_crm.run` starts it
by hand. Run only one at a time: two share the state file and overwrite each other.

## Marketing automation

The CRM runs marketing automation off the engagement tier it holds:

- The re-engagement campaign fires on the **transition into** the `dormant` tier. It
  reacts to the change, not to the value: writing `dormant` to a member who is already
  `dormant` sends nothing.
- **No automation fires on the transition out of `dormant`.**
- **A correcting write does not un-send an email that has already gone out.** There is
  no endpoint to recall a sent email; the record is append-only.

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/automation/emails` | Emails the automation has sent. |
| `GET` | `/v1/automation/transitions` | Every tier transition the CRM has recorded. |

## Members

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/members?offset=&limit=` | One page of member records, ordered by member id, at most 500 per page. `next_offset` is `null` on the last page. |
| `GET` | `/v1/members/{id}` | One member record. `404` if the CRM has no record for them. |
| `PATCH` | `/v1/members/{id}` | Merge the posted properties into the member's record. Sending the same properties again changes nothing. |
| `GET` | `/v1/members/{id}/notes` | The member's timeline notes. |
| `POST` | `/v1/members/{id}/notes` | Append a timeline note. |

## Sync runs

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/sync/runs` | Runs the CRM has been told about. |
| `POST` | `/v1/sync/runs` | Record that a sync run completed. |

Each sync run may make up to 30,000 requests; beyond that the API returns `429`.

## The service itself, and testing

| Method | Path | Notes |
|---|---|---|
| `GET` | `/` | What this instance is: state file, rate limit, and the routes it serves. |
| `GET` | `/health` | `{"status": "ok"}` while the store is readable. |
| `POST` | `/v1/admin/fail_next` | `{"path": "...", "times": n}`: make the next `n` requests to that path fail with a `500`, so an unhappy path can be demonstrated. |

The other `/v1/admin/` endpoints the index lists exist for the replay and reset scripts.
