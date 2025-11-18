from airflow.decorators import dag, task
from datetime import datetime
import os

DEFAULT_ARGS = {
    "owner": "data_platform",
    "retries": 1,
}

@dag(
    schedule="@daily",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["apolitical"],
)
def pipeline():
    """Orchestrates dbt models for course engagement analytics.

    TODO for candidate:
    - Deside on the DAG structure and Tasks
      - Current tasks are just placeholders, add or remove based on your design
      - Should use a sensible structure
      - Tasks should refresh the Data Models AND output a report CSV file
    - Bonus
      - Add data quality tests and checks
      - Add logging or error handling you think is appropriate
    """

    @task
    def run_staging():
        """Run dbt staging models to clean and deduplicate raw data."""
        # TODO: replace with your preferred way of invoking dbt, e.g.:
        # subprocess.run(["dbt", "run", "--select", "path:models/staging"], check=True)
        pass

    @task
    def run_intermediate():
        """Run dbt intermediate models to join and transform data."""
        pass

    @task
    def run_marts():
        """Run dbt marts models to create final analytics tables."""
        pass

    @task
    def report_data(**context):
        """Export mart_course_engagement table to CSV for stakeholders.

        TODO: Implement CSV export with:
        - Connect to DuckDB at ../mock_data.duckdb
        - Query analytics.mart_course_engagement table
        - Export to output/course_engagement_20251201.csv (date shouild be the DAG run date)
        - Create output directory if it doesn't exist
        """
        pass

    # Define task dependencies
    staging = run_staging()
    intermediate = run_intermediate()
    marts = run_marts()
    report = report_data()

    # Set up the DAG flow
    # staging -> intermediate -> marts -> report
    staging >> intermediate >> marts >> report

pipeline()
