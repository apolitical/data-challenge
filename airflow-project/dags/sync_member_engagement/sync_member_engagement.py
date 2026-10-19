# dags/sync_member_engagement/sync_member_engagement.py
from datetime import timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable
from utilities.common import get_dag_configs

from sync_member_engagement import __runtime__, __start_date__
from sync_member_engagement.crm_client import sync_engagement_tiers

REPO_ROOT = Path(__file__).resolve().parents[3]

dag_id = "sync-member-engagement"

dag_args = {
    "start_date": __start_date__,
    "end_date": None,
    "schedule": CronTriggerTimetable(
        "0 2 * * *", timezone="UTC", interval=timedelta(days=1)
    )
    if __runtime__ in ("production", "test")
    else None,
    "is_paused_upon_creation": None if __runtime__ == "production" else True,
    "catchup": False,
    "dagrun_timeout": timedelta(hours=1),
    "max_active_runs": 1,
    "tags": ["crm", "data-sync", "modifies-live"],
    "render_template_as_native_obj": True,
}

default_task_args = {
    "owner": "apolitical",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "trigger_rule": "all_success",
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(dag_id=dag_id, default_args=default_task_args, **dag_args)  # type: ignore
def sync_member_engagement_pipeline():
    """
    sync-member-engagement

    Builds member engagement tiers in the warehouse and pushes them into the CRM.
    Marketing automation in the CRM is driven off the tier it holds, so what this DAG
    writes reaches members.

    Pipeline steps:
    1. Build the engagement models for the run date.
    2. Run the critical data tests.
    3. Write every member's tier to the CRM.

    Required Connections:
      - none; the CRM's address and token come from the environment
    Required Variables:
      - none
    """
    configs = get_dag_configs("sync_member_engagement", __runtime__)
    common = configs["common"]

    dbt_dir = REPO_ROOT / common["dbt_project_dir"]
    dbt_env = {"DBT_PROFILES_DIR": str(dbt_dir)}

    build_models = BashOperator(
        task_id="build-models",
        bash_command=(
            f"cd {dbt_dir} && dbt run --vars '{{\"run_date\": \"{{{{ ds }}}}\"}}'"
        ),
        env=dbt_env,
        append_env=True,
    )

    run_critical_tests = BashOperator(
        task_id="run-critical-tests",
        bash_command=(
            f"cd {dbt_dir} && dbt test --select {common['test_selector']} "
            f"--vars '{{\"run_date\": \"{{{{ ds }}}}\"}}'"
        ),
        env=dbt_env,
        append_env=True,
    )

    @task(task_id="push-to-crm")
    def push_to_crm(**context):
        """Read the engagement mart and write every member's tier to the CRM."""
        import duckdb

        warehouse = REPO_ROOT / common["warehouse_file"]
        conn = duckdb.connect(str(warehouse), read_only=True)
        try:
            rows = conn.execute(
                f"""
                SELECT
                    member_id,
                    engagement_tier,
                    previous_tier
                  FROM
                    analytics.{common['engagement_model']}
                """
            ).fetchall()
        finally:
            conn.close()

        payload = [
            {"member_id": member_id, "engagement_tier": tier, "previous_tier": previous}
            for member_id, tier, previous in rows
        ]
        return sync_engagement_tiers(payload, run_id=context["run_id"])

    chain(build_models, run_critical_tests, push_to_crm())


result_dag = sync_member_engagement_pipeline()
