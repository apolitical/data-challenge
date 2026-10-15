"""Alerting callbacks for DAG failures.

Every DAG sets `on_failure_callback=slack_on_failure`, so that a failed task reaches a
human rather than only the task log.

**This copy writes to the local log and nowhere else.** In the real deployment the
callback posts to Slack; here there is no outbound request of any kind, no webhook to
configure, and no way for a failure alert to leave this machine. Nothing in the
pipeline can reach an external service.
"""

import logging
import os

logger = logging.getLogger(__name__)

ALERT_CHANNEL = "#data-alerts"


def slack_on_failure(context):
    """Record a failed task where the alert would have gone.

    Deliberately tolerant: an alerting callback that raises would mask the failure it
    was called to report.
    """
    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    run_id = context.get("run_id", "unknown")
    runtime = os.environ.get("APOLITICAL_RUNTIME", "local")

    try:
        logger.warning(
            "ALERT -> %s [%s]: %s.%s failed in run %s",
            ALERT_CHANNEL,
            runtime,
            dag_id,
            task_id,
            run_id,
        )
    except Exception:
        logging.getLogger(__name__).exception(
            "Could not record the failure alert for %s.%s", dag_id, task_id
        )
