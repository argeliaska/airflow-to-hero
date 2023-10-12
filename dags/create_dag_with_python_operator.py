from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

def greet(age, ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    print(f"Hello World! My name is {first_name} {last_name} and I am {age} years old!")

def get_name(ti): # ti = task instance
    ti.xcom_push(key='first_name', value='Alex')
    ti.xcom_push(key='last_name', value='Skakie')

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}
with DAG(
    dag_id='our_first_python_dag_v05',
    default_args=default_args,
    description='Our first dag using python operator',
    start_date=datetime(2023,10,11),
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'age':18}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )
    task2 >> task1