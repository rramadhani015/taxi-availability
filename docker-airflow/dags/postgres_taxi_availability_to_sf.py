from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from functions.snowflake_function import (insert_taxi_general,insert_taxi_geometry)

# ========== DAG ==========
with DAG(
    dag_id="postgres_taxi_availability_to_sf",
    description="Ingest data from Postgres to Snowflake (general, geometry, location)",
    schedule_interval="0 * * * *",  # hourly
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)}
) as dag:

    t1_general = PythonOperator(
        task_id="insert_taxi_general",
        python_callable=insert_taxi_general,
        provide_context=True
    )

    t2_geometry = PythonOperator(
        task_id="insert_taxi_geometry",
        python_callable=insert_taxi_geometry
    )

    # scaling issue
    # t3_location = PythonOperator(
    #     task_id="insert_taxi_location",
    #     python_callable=insert_taxi_location
    # )

    [t1_general, t2_geometry]  # run parallel
    # [t3_location]  # run parallel
