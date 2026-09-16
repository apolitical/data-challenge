"""Tests for the mock CRM service.

The mock CRM stands in for a real CRM in this challenge. It is the one piece of the
repository whose behaviour candidates should be able to trust, so its observable
behaviour is tested directly: what persists, what duplicates, what the rate limit
does, and which writes trigger the marketing automation.
"""

import json
import time

import pytest

from mock_crm.service import create_app, CrmStore


@pytest.fixture
def store(tmp_path):
    return CrmStore(tmp_path / "state.json")


@pytest.fixture
def client(store):
    return create_app(store).test_client()


def test_member_property_write_persists_across_restart(tmp_path):
    """A PATCH of a member's properties survives the service being restarted.

    The pipeline runs daily and the CRM is the system of record between runs, so state
    that vanished on restart would make the incident history unreproducible.
    """
    state_file = tmp_path / "state.json"
    first = create_app(CrmStore(state_file)).test_client()
    first.patch("/v1/members/42", json={"engagement_tier": "dormant"})

    reopened = create_app(CrmStore(state_file)).test_client()
    response = reopened.get("/v1/members/42")

    assert response.status_code == 200
    assert response.get_json()["properties"]["engagement_tier"] == "dormant"


def test_posting_the_same_timeline_note_twice_leaves_two_notes(client):
    """Timeline notes append; they are not upserted.

    A property PATCH is idempotent, so re-running the sync over unchanged data is
    harmless for properties. A note is not: posting it again adds another one. This
    asymmetry is what makes a retried sync member-visible.
    """
    note = {"body": "Engagement tier changed from engaged to dormant"}
    client.post("/v1/members/42/notes", json=note)
    client.post("/v1/members/42/notes", json=note)

    notes = client.get("/v1/members/42/notes").get_json()["notes"]

    assert len(notes) == 2


def test_transition_into_dormant_fires_the_re_engagement_email(client):
    """Marketing automation fires on the transition into `dormant`, not on the value.

    This is the behaviour that turns a wrong tier into a wrong email. It fires on the
    change, so writing `dormant` to a member who is already dormant sends nothing.
    """
    client.patch("/v1/members/42", json={"engagement_tier": "engaged"})
    client.patch("/v1/members/42", json={"engagement_tier": "dormant"})
    client.patch("/v1/members/42", json={"engagement_tier": "dormant"})

    sent = client.get("/v1/automation/emails").get_json()["emails"]

    assert [(e["member_id"], e["campaign"]) for e in sent] == [("42", "re_engagement")]


def test_transition_out_of_dormant_fires_nothing_and_leaves_the_email_sent(client):
    """Correcting a wrong tier does not un-send the email it caused.

    There is no automation on the way out of `dormant`, and the email record is
    append-only. This is why the recovery question in the incident is about apologising
    rather than about reversing.
    """
    client.patch("/v1/members/42", json={"engagement_tier": "dormant"})
    client.patch("/v1/members/42", json={"engagement_tier": "engaged"})

    sent = client.get("/v1/automation/emails").get_json()["emails"]
    transitions = client.get("/v1/automation/transitions").get_json()["transitions"]

    assert len(sent) == 1, "the re-engagement email stays sent"
    assert [(t["from"], t["to"]) for t in transitions] == [(None, "dormant"), ("dormant", "engaged")]


def test_requests_beyond_the_rate_limit_are_rejected_with_429(tmp_path):
    """The rate limit is real and reads count against it.

    Reading current CRM state before writing is the strongest safety control available
    to this pipeline, and this is what gives that control a price the candidate has to
    weigh rather than an unlimited free resource.
    """
    store = CrmStore(tmp_path / "state.json", rate_limit=3)
    client = create_app(store).test_client()

    codes = [client.get("/v1/members/42").status_code for _ in range(5)]

    assert codes.count(429) == 2, f"expected two rejections, got {codes}"
    assert client.get("/v1/members/42").get_json()["error"] == "rate_limited"


def test_reset_restores_the_baseline_and_discards_everything_since(tmp_path):
    """Reset returns the CRM to its recorded baseline: state, notes, emails and all.

    The candidate needs to be able to run the incident, break something, and start
    again from the same place. Without that, a single destructive experiment ends the
    exercise.
    """
    state_file = tmp_path / "state.json"
    store = CrmStore(state_file)
    client = create_app(store).test_client()

    client.patch("/v1/members/42", json={"engagement_tier": "engaged"})
    client.post("/v1/admin/baseline")

    client.patch("/v1/members/42", json={"engagement_tier": "dormant"})
    client.post("/v1/members/42/notes", json={"body": "tier changed"})
    assert len(client.get("/v1/automation/emails").get_json()["emails"]) == 1

    client.post("/v1/admin/reset")

    assert client.get("/v1/members/42").get_json()["properties"]["engagement_tier"] == "engaged"
    assert client.get("/v1/members/42/notes").get_json()["notes"] == []
    assert client.get("/v1/automation/emails").get_json()["emails"] == []


def test_an_injected_failure_fails_the_named_endpoint_once_then_recovers(client):
    """Failures can be injected, so unhappy paths are demonstrable rather than theoretical.

    Two things in this repository can only be shown with a failure that happens on
    demand: whether a failed write is visible in the run's outcome, and whether a
    retried sync duplicates anything it had already written.
    """
    client.post("/v1/admin/fail_next", json={"path": "/v1/sync/runs", "times": 1})

    first = client.post("/v1/sync/runs", json={"run_id": "manual__1"})
    second = client.post("/v1/sync/runs", json={"run_id": "manual__1"})

    assert first.status_code == 500
    assert second.status_code == 201


def test_listing_members_is_paginated_with_a_capped_page_size(client):
    """Current state can be read in bulk, a page at a time.

    A page is capped, so reading the whole population costs several requests -- but far
    fewer than reading each member individually. Which of those a sync does is the
    difference between a strategy that fits the rate limit and one that does not.
    """
    for member_id in range(1, 8):
        client.patch(f"/v1/members/{member_id}", json={"engagement_tier": "engaged"})

    first = client.get("/v1/members?limit=5").get_json()
    second = client.get(f"/v1/members?offset={first['next_offset']}&limit=5").get_json()

    assert len(first["members"]) == 5
    assert first["total"] == 7
    assert len(second["members"]) == 2
    assert second["next_offset"] is None


def test_starting_a_new_window_restores_the_full_quota(tmp_path):
    """The rate limit is per window, and each daily run is a new window.

    Without this the replayed run history would exhaust one window's quota partway
    through the second day, which is an artefact of replaying four runs in one process
    rather than anything about the pipeline.
    """
    client = create_app(CrmStore(tmp_path / "state.json", rate_limit=2)).test_client()

    assert [client.get("/v1/members").status_code for _ in range(3)] == [200, 200, 429]

    client.post("/v1/admin/new_window")

    assert client.get("/v1/members").status_code == 200


def test_writes_stay_fast_against_a_full_size_population(tmp_path):
    """Persisting a write must not cost a full re-serialisation of the population.

    A sync run writes every one of about 12,000 members, twice. At 23ms per write --
    which is what re-serialising the whole snapshot each time costs -- one run takes
    nine minutes, and the candidate spends their two hours waiting on our mock instead
    of on the problem. The bound here is deliberately loose so it measures the
    algorithm rather than the machine.
    """
    store = CrmStore(tmp_path / "state.json")
    for member_id in range(1, 12_001):
        store._member_record(member_id)["properties"]["engagement_tier"] = "engaged"
    store.flush()

    started = time.time()
    for member_id in range(1, 201):
        store.update_properties(member_id, {"engagement_tier": "dormant"})
    elapsed = time.time() - started

    per_write_ms = elapsed / 200 * 1000
    assert per_write_ms < 5, f"{per_write_ms:.1f} ms per write against 12,000 members"


def test_recording_a_sync_run_starts_the_next_rate_limit_window(tmp_path):
    """Completing a sync run opens a fresh window, as the documented limit implies.

    The limit is per run. Without this, a second ordinary run against a CRM that has
    been up since the first one shares the first run's quota, starts taking 429s partway
    through, and the cause is our mock rather than anything the pipeline did.
    """
    client = create_app(CrmStore(tmp_path / "state.json", rate_limit=2)).test_client()

    assert [client.get("/v1/members").status_code for _ in range(3)] == [200, 200, 429]

    client.post("/v1/sync/runs", json={"run_id": "scheduled__first"})

    assert client.get("/v1/members").status_code == 200


def test_compacting_makes_the_snapshot_self_contained(tmp_path):
    """After compaction the snapshot alone holds everything, with no journal needed.

    Writes land in an append-only journal and are folded into the snapshot only when the
    journal grows large. That is fine while the service is running, because reopening the
    store replays the journal -- but the delivered artefact ships the snapshot, and a
    snapshot that is only complete when read together with a file nobody ships is a
    snapshot that silently loses most of the incident.
    """
    state_file = tmp_path / "state.json"
    store = CrmStore(state_file)
    client = create_app(store).test_client()
    for member_id in range(1, 26):
        client.patch(f"/v1/members/{member_id}", json={"engagement_tier": "dormant"})

    client.post("/v1/admin/compact")

    assert not store.journal_path.exists(), "the journal should be gone after compaction"
    snapshot = json.loads(state_file.read_text())
    assert len(snapshot["members"]) == 25
    assert len(snapshot["emails"]) == 25


def test_the_service_root_returns_an_index_of_the_api(client):
    """GET / describes the running service instead of returning Flask's 404 page.

    The API is entirely under /v1, so a browser opened at the service's address used to
    get a bare "Not Found". That is indistinguishable from a broken server, and the
    brief promises that scaffold problems are ours rather than the candidate's. The
    index names the environment, the state file it is writing to -- which is how you
    tell two instances apart -- and the routes that exist.
    """
    response = client.get("/")

    assert response.status_code == 200
    body = response.get_json()
    assert body["service"] == "mock-crm"
    assert body["state_file"].endswith("state.json")
    assert "GET /v1/automation/emails" in body["routes"]


def test_the_index_and_health_check_do_not_consume_sync_quota(tmp_path):
    """Looking at the service costs nothing against the rate limit.

    The limit is a per-run quota for the pipeline. Flask runs before_request after
    routing, so every liveness poll -- and every browser refresh that used to 404 --
    spent one of the pipeline's requests, and a candidate who left a tab reloading
    would have watched the sync take 429s for no reason they could see.
    """
    client = create_app(CrmStore(tmp_path / "state.json", rate_limit=2)).test_client()

    for _ in range(5):
        assert client.get("/").status_code == 200
        assert client.get("/health").status_code == 200

    assert [client.get("/v1/members").status_code for _ in range(3)] == [200, 200, 429]


def test_the_health_check_reports_how_many_members_the_crm_holds(client):
    """/health's member count is the population, not the shape of a page response.

    list_members() returns a page envelope -- members, total, offset, next_offset -- so
    counting the envelope itself reports 4 for any CRM in any state, which looks like a
    working liveness check and tells you nothing.
    """
    for member_id in (7, 8, 9):
        client.patch(f"/v1/members/{member_id}", json={"engagement_tier": "engaged"})

    assert client.get("/health").get_json()["members"] == 3
