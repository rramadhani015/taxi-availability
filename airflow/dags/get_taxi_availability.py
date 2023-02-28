from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from functions.taxi_availability import (run_taxi_api_to_db)

#leave in comment when in production
# default_args = {
# 	'email_on_failure': False
# }

with DAG(
    'get_taxi_availability',
    description='Extract and transform data of taxi availability',
    schedule_interval='*/1 * * * *',
    start_date=days_ago(1),
    catchup=False
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    finish = DummyOperator(
        task_id='finish'
    )

# ============================================================
    taxi_api_to_db = PythonOperator(
        task_id='taxi_api_to_db',
        python_callable=run_taxi_api_to_db,
        retries=2,
        retry_delay=timedelta(seconds=30)
    )


# ============================================================
    start >> taxi_api_to_db >> finish
