from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from functions.dq_function import (dq_check_and_log)

# ========== DAG ==========
with DAG(
    dag_id="dq_check_taxi",
    description="DQ Check for Taxi tables with Snowflake logging",
    schedule_interval='0 */6 * * *',  # every 6 hours
    start_date=days_ago(1),
    catchup=False
) as dag:

    dq_check_task = PythonOperator(
        task_id="dq_check_and_log",
        python_callable=dq_check_and_log,
        provide_context=True
    )

    [dq_check_task]
