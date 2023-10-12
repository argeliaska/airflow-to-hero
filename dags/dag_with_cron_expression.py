from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_cron_expression_v02',
    default_args=default_args,
    start_date=datetime(2023,9,30),
    schedule_interval='0 3 * * TUE',
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo DAG with cron expression!"
    )