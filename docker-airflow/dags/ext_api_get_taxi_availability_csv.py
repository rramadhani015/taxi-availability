from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from functions.taxi_availability_csv import run_taxi_api_to_csv

# ============================================================
# DAG: ext_api_get_taxi_availability_csv
# This DAG does not run on schedule.
# It is triggered programmatically from another DAG.
# ============================================================

with DAG(
    'ext_api_get_taxi_availability_csv',
    description='Extract data from table to csv',
    schedule_interval=None,  # <-- Disable automatic scheduling
    start_date=days_ago(1),
    catchup=False
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    finish = DummyOperator(
        task_id='finish'
    )

    taxi_db_to_csv = PythonOperator(
        task_id='taxi_db_to_csv',
        python_callable=run_taxi_api_to_csv,
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    start >> taxi_db_to_csv >> finish
