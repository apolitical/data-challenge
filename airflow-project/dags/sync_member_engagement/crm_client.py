"""Client for the CRM's member API.

Used by the sync-member-engagement DAG to push engagement tiers into the CRM.
Configuration comes from the environment:

    CRM_BASE_URL   the CRM to write to
    CRM_API_TOKEN  the API token to authenticate with
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)

TIMEOUT_SECONDS = 10


class CrmClient:
    """Reads and writes member records in the CRM."""

    def __init__(self, base_url=None, api_token=None):
        self.base_url = (base_url or os.environ["CRM_BASE_URL"]).rstrip("/")
        self.api_token = api_token or os.environ.get("CRM_API_TOKEN", "")
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {self.api_token}"

    def get_member(self, member_id):
        """Return the member's current CRM record, or None if the CRM has no record."""
        response = self.session.get(
            f"{self.base_url}/v1/members/{member_id}", timeout=TIMEOUT_SECONDS
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def set_engagement_tier(self, member_id, tier):
        """Write the member's engagement tier."""
        response = self.session.patch(
            f"{self.base_url}/v1/members/{member_id}",
            json={"engagement_tier": tier},
            timeout=TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return response.json()

    def add_timeline_note(self, member_id, body):
        """Add a note to the member's CRM timeline."""
        response = self.session.post(
            f"{self.base_url}/v1/members/{member_id}/notes",
            json={"body": body},
            timeout=TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return response.json()

    def complete_run(self, run_id):
        """Tell the CRM the sync run finished."""
        response = self.session.post(
            f"{self.base_url}/v1/sync/runs", json={"run_id": run_id}, timeout=TIMEOUT_SECONDS
        )
        response.raise_for_status()
        return response.json()


def sync_engagement_tiers(rows, run_id, client=None):
    """Push each member's engagement tier into the CRM.

    `rows` is an iterable of mappings with member_id, engagement_tier and previous_tier.
    """
    client = client or CrmClient()
    written = 0

    for row in rows:
        member_id = row["member_id"]
        tier = row["engagement_tier"]
        try:
            client.set_engagement_tier(member_id, tier)
            client.add_timeline_note(
                member_id,
                f"Engagement tier set to {tier} by run {run_id}",
            )
            written += 1
        except Exception:
            logging.warning("Could not sync member %s", member_id)

    logger.info("Synced %s of %s members", written, len(rows))
    client.complete_run(run_id)
    return written
