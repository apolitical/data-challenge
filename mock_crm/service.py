"""A local mock of the CRM the engagement pipeline writes to.

Persists to a JSON file so that state survives between pipeline runs, which is what
makes the incident history reproducible and the recovery exercise real.

Nothing here talks to a real service. Both environments this repository starts are
local mocks with dummy credentials.
"""

import json
from pathlib import Path

from flask import Flask, jsonify, request


class CrmStore:
    """Member state, persisted to a JSON snapshot plus an append-only journal.

    A sync run writes every member twice, so persisting each change by re-serialising
    the whole population would cost about 23ms per write -- nine minutes for one run
    against 12,000 members. Instead every change appends one line to a journal, which
    is O(1) in the size of the population, and the snapshot is rewritten only when the
    journal has grown large enough to be worth compacting.

    Durability is unchanged. A change is on disk before the request returns, and
    reopening the store replays any journal entries the snapshot does not yet contain.
    """

    # Journal entries to accumulate before folding them back into the snapshot.
    COMPACT_AFTER = 50_000

    # Requests allowed per sync window, matching the documented limit in
    # mock_crm/API.md. A full unconditional sync of every member spends most of
    # this, so any strategy that also reads before writing has to be deliberate
    # about how much it reads.
    DEFAULT_RATE_LIMIT = 30_000

    def __init__(self, path, rate_limit=None):
        self.path = Path(path)
        self.rate_limit = self.DEFAULT_RATE_LIMIT if rate_limit is None else rate_limit
        self._requests = 0
        self.baseline_path = self.path.with_suffix(".baseline.json")
        # Injected failures are deliberately in-memory: they are a testing aid,
        # not part of the CRM's state, and must not survive a restart.
        self._failures = {}
        self.journal_path = self.path.with_suffix(".journal.jsonl")
        self._journal_entries = 0
        self._state = {"members": {}, "emails": [], "transitions": [], "runs": []}
        if self.path.exists():
            self._state.update(json.loads(self.path.read_text()))
        self._replay_journal()

    # Largest page the member list will return, however large a limit is asked for.
    MAX_PAGE_SIZE = 500

    def member(self, member_id):
        return self._state["members"].get(str(member_id))

    def list_members(self, offset=0, limit=None):
        """Return one page of member records, ordered by member id.

        Ordering is by numeric member id so that paging through the population is
        stable while the sync is writing to it.
        """
        size = min(limit or self.MAX_PAGE_SIZE, self.MAX_PAGE_SIZE)
        ids = sorted(self._state["members"], key=int)
        page = ids[offset : offset + size]
        return {
            "members": {member_id: self._state["members"][member_id] for member_id in page},
            "total": len(ids),
            "offset": offset,
            "next_offset": offset + size if offset + size < len(ids) else None,
        }

    # The tier whose arrival the marketing automation reacts to.
    AUTOMATION_TIER = "dormant"
    AUTOMATION_CAMPAIGN = "re_engagement"

    def update_properties(self, member_id, properties):
        member = self._member_record(member_id)
        before = member["properties"].get("engagement_tier")
        member["properties"].update(properties)
        after = member["properties"].get("engagement_tier")
        self._journal("properties", {"member_id": str(member_id), "properties": properties})
        if after != before:
            self._record_transition(member_id, before, after)
        return member

    def _record_transition(self, member_id, before, after):
        """Record the tier change and run the automation that reacts to it.

        The automation fires on the *transition into* the dormant tier. Writing the
        same tier again is not a transition and sends nothing, and a transition out of
        dormant triggers no automation at all -- so a correcting write does not undo
        an email that has already gone out.
        """
        transition = {"member_id": str(member_id), "from": before, "to": after}
        self._state["transitions"].append(transition)
        self._journal("transitions", {"entry": transition})
        if after == self.AUTOMATION_TIER:
            email = {"member_id": str(member_id), "campaign": self.AUTOMATION_CAMPAIGN}
            self._state["emails"].append(email)
            self._journal("emails", {"entry": email})

    def record_run(self, run_id):
        """Record that a sync run completed. The pipeline calls this last.

        Completing a run also opens the next rate-limit window. The limit is documented
        as per-run, and this is the only signal the CRM gets that one run has ended --
        without it a second run against a long-lived service inherits the first run's
        spent quota and starts taking 429s for no reason the pipeline could see.
        """
        entry = {"run_id": run_id, "sequence": len(self._state["runs"]) + 1}
        self._state["runs"].append(entry)
        self._journal("runs", {"entry": entry})
        self.new_window()
        return entry

    def runs(self):
        return self._state["runs"]

    def inject_failure(self, path, times):
        """Make the next `times` requests to `path` fail with a 500."""
        self._failures[path] = times
        return {"path": path, "times": times}

    def should_fail(self, path):
        remaining = self._failures.get(path, 0)
        if remaining <= 0:
            return False
        self._failures[path] = remaining - 1
        return True

    def save_baseline(self):
        """Record the current state as the point `reset` returns to."""
        self.baseline_path.write_text(json.dumps(self._state, indent=2, sort_keys=True))
        return {"baseline": str(self.baseline_path)}

    def reset(self):
        """Discard everything since the baseline: properties, notes and emails alike."""
        if self.baseline_path.exists():
            self._state = json.loads(self.baseline_path.read_text())
        else:
            self._state = {"members": {}, "emails": [], "transitions": [], "runs": []}
        self._requests = 0
        self._flush()
        return {"reset_to": "baseline" if self.baseline_path.exists() else "empty"}

    def consume_request(self):
        """Count one request against the rate limit. False once the limit is spent.

        Reads count as well as writes, so a diff-before-write strategy costs twice.
        """
        self._requests += 1
        return self._requests <= self.rate_limit

    def requests_made(self):
        return self._requests

    def new_window(self):
        """Begin a new rate-limit window, restoring the full quota."""
        self._requests = 0
        return {"rate_limit": self.rate_limit, "requests_made": 0}

    def emails(self):
        return self._state["emails"]

    def transitions(self):
        return self._state["transitions"]

    def append_note(self, member_id, body):
        """Append a timeline note. Notes accumulate; they are never replaced."""
        member = self._member_record(member_id)
        note = {"note_id": len(member["notes"]) + 1, "body": body}
        member["notes"].append(note)
        self._journal("note", {"member_id": str(member_id), "note": note})
        return note

    def notes(self, member_id):
        member = self.member(member_id)
        return member["notes"] if member else []

    def _member_record(self, member_id):
        return self._state["members"].setdefault(
            str(member_id), {"properties": {}, "notes": []}
        )

    def flush(self):
        """Write a full snapshot and discard the journal it supersedes."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._state, indent=2, sort_keys=True))
        self.journal_path.unlink(missing_ok=True)
        self._journal_entries = 0

    def _journal(self, kind, payload):
        """Append one change to the journal, compacting when it has grown large."""
        self.journal_path.parent.mkdir(parents=True, exist_ok=True)
        with self.journal_path.open("a") as fh:
            fh.write(json.dumps({"kind": kind, **payload}) + "\n")
            fh.flush()
        self._journal_entries += 1
        if self._journal_entries >= self.COMPACT_AFTER:
            self.flush()

    def _replay_journal(self):
        """Apply journal entries the snapshot does not yet contain."""
        if not self.journal_path.exists():
            return
        for line in self.journal_path.read_text().splitlines():
            if not line.strip():
                continue
            entry = json.loads(line)
            kind = entry.pop("kind")
            if kind == "properties":
                record = self._member_record(entry["member_id"])
                record["properties"].update(entry["properties"])
            elif kind == "note":
                self._member_record(entry["member_id"])["notes"].append(entry["note"])
            elif kind in ("emails", "transitions", "runs"):
                self._state[kind].append(entry["entry"])
        self._journal_entries = sum(
            1 for line in self.journal_path.read_text().splitlines() if line.strip()
        )

    # Retained so existing call sites keep working; a full flush is the safe default
    # for anything that is not a per-member change.
    def _flush(self):
        self.flush()


def create_app(store):
    app = Flask(__name__)

    @app.get("/")
    def index():
        """Describe this running instance: what it is, where its state lives, what it serves.

        The API lives entirely under /v1, so without this a browser pointed at the
        service returns Flask's bare 404 and there is no way to tell a working mock from
        a broken one. The state file is included because the environment -- not the port
        -- chooses it, so this is how you tell two instances apart.
        """
        routes = sorted(
            f"{method} {rule.rule}"
            for rule in app.url_map.iter_rules()
            for method in rule.methods - {"HEAD", "OPTIONS"}
            if rule.endpoint != "static"
        )
        return jsonify(
            {
                "service": "mock-crm",
                "environment": store.path.stem,
                "state_file": str(store.path),
                "rate_limit": store.rate_limit,
                "docs": "mock_crm/API.md",
                "routes": routes,
            }
        ), 200

    @app.get("/health")
    def health():
        """Liveness check: 200 while the store is readable, with the journal depth."""
        return jsonify(
            {
                "status": "ok",
                "environment": store.path.stem,
                "members": store.list_members()["total"],
            }
        ), 200

    @app.get("/v1/members")
    def list_members():
        offset = request.args.get("offset", default=0, type=int)
        limit = request.args.get("limit", default=None, type=int)
        return jsonify(store.list_members(offset=offset, limit=limit)), 200

    @app.get("/v1/members/<member_id>")
    def get_member(member_id):
        member = store.member(member_id)
        if member is None:
            return jsonify({"error": "not_found"}), 404
        return jsonify(member), 200

    @app.patch("/v1/members/<member_id>")
    def patch_member(member_id):
        member = store.update_properties(member_id, request.get_json(silent=True) or {})
        return jsonify(member), 200

    @app.get("/v1/members/<member_id>/notes")
    def get_notes(member_id):
        return jsonify({"notes": store.notes(member_id)}), 200

    @app.post("/v1/members/<member_id>/notes")
    def post_note(member_id):
        payload = request.get_json(silent=True) or {}
        note = store.append_note(member_id, payload.get("body", ""))
        return jsonify(note), 201

    @app.get("/v1/automation/emails")
    def get_emails():
        return jsonify({"emails": store.emails()}), 200

    @app.get("/v1/automation/transitions")
    def get_transitions():
        return jsonify({"transitions": store.transitions()}), 200

    @app.get("/v1/sync/runs")
    def get_runs():
        return jsonify({"runs": store.runs()}), 200

    @app.post("/v1/sync/runs")
    def post_run():
        payload = request.get_json(silent=True) or {}
        return jsonify(store.record_run(payload.get("run_id", ""))), 201

    @app.post("/v1/admin/compact")
    def post_compact():
        store.flush()
        return jsonify({"compacted": True, "snapshot": str(store.path)}), 200

    @app.post("/v1/admin/new_window")
    def post_new_window():
        return jsonify(store.new_window()), 200

    @app.post("/v1/admin/fail_next")
    def post_fail_next():
        payload = request.get_json(silent=True) or {}
        return jsonify(store.inject_failure(payload["path"], int(payload.get("times", 1)))), 200

    @app.post("/v1/admin/baseline")
    def post_baseline():
        return jsonify(store.save_baseline()), 200

    @app.post("/v1/admin/reset")
    def post_reset():
        return jsonify(store.reset()), 200

    @app.before_request
    def enforce_rate_limit_and_injected_failures():
        # The admin endpoints are this mock's own control surface, not part of the CRM
        # API the pipeline talks to, so they neither consume quota nor fail. The index
        # and the health check are the same kind of thing: looking at the service is not
        # the pipeline using its quota, and Flask would otherwise charge a request for
        # every browser refresh, because before_request runs even for a 404.
        if request.path.startswith("/v1/admin/") or request.path in {"/", "/health"}:
            return None

        # Recording a completed run closes the rate-limit window, so it is not itself
        # refused for being over the limit -- a run that had spent its quota would
        # otherwise have no way to say it had finished, and every later run would
        # inherit the exhausted window. It remains subject to injected failures, which
        # is what makes a failing run completion reproducible.
        closes_window = request.method == "POST" and request.path == "/v1/sync/runs"
        if not closes_window and not store.consume_request():
            return jsonify({"error": "rate_limited", "limit": store.rate_limit}), 429

        if store.should_fail(request.path):
            return jsonify({"error": "internal_error"}), 500
        return None

    return app
