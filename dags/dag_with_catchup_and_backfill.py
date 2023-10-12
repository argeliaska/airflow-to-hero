from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# the catchup parameter of the DAG was set to False, 
# but we can still run the DAG in the past using the backfill command
# In the airflow (scheduler) container execute the command
# airflow dags backfill with start date, end date and dag id
# airflow dags backfill -s 2023-10-01 -e 2023-10-07 dag_with_catchup_backfill_v03

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    dag_id='dag_with_catchup_backfill_v03',
    default_args=default_args,
    start_date=datetime(2023,9,30),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo This is a simple bash command!"
    )