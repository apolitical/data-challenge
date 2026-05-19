# 3-airflow/dags/pipeline.py
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from datetime import datetime

DEFAULT_ARGS = {
    "owner": "data_platform",
    "max_active_runs_per_dag": 1,
    "retries": 1,
}

@dag(
    schedule="@daily",
    start_date=datetime(2025, 12, 14),
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def apolitical_data_challenge():
    """Orchestrates dbt models for course engagement analytics.

    TODO for candidate:
    - Decide on the DAG structure and Tasks
      - Current tasks are just placeholders, add or remove based on your design
      - Should use a sensible structure
      - Tasks should refresh the Data Models AND output a report CSV file
    - Bonus
      - Add data quality tests and checks
      - Add logging or error handling you think is appropriate
    """

    @task
    def run_base():
        """Run dbt base models to clean and deduplicate raw data."""
        # TODO: replace with your preferred way of invoking dbt, e.g.:
        # subprocess.run(
        #     ["dbt", "run", "--select", "path:models/base"],
        #     check=True
        # )
        pass

    @task
    def run_intermediate():
        """Run dbt intermediate models to join and transform data."""
        raise AirflowFailException("Nope, Task not yet configured")

    @task
    def run_marts():
        """Run dbt marts models to create final analytics tables."""
        raise AirflowFailException("Nope, Task not yet configured")

    @task
    def report_data(**context):
        """Export marts_courses__engagement table to CSV for stakeholders.

        TODO: Implement CSV export with:
        - Connect to DuckDB at ../mock_data.duckdb
        - Query analytics.marts_courses__engagement table
        - Export to output/reports/course_engagement_20251201.csv (date should be the DAG run date)
        - Create output directory if it doesn't exist
        """
        raise AirflowFailException("Nope, Task not yet configured")

    # Define task dependencies
    base = run_base()
    intermediate = run_intermediate()
    marts = run_marts()
    report = report_data()

    # Set up the DAG flow
    # base -> intermediate -> marts -> report
    base >> intermediate >> marts >> report

apolitical_data_challenge()
