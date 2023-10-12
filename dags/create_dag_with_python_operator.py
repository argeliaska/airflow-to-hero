from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello World! My name is {first_name} {last_name} and I am {age} years old!")

def get_name(ti): # ti = task instance
    ti.xcom_push(key='first_name', value='Alex')
    ti.xcom_push(key='last_name', value='Skakie')

def get_age(ti): 
    ti.xcom_push(key='age', value='20')

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}
with DAG(
    dag_id='our_first_python_dag_v06',
    default_args=default_args,
    description='Our first dag using python operator',
    start_date=datetime(2023,10,11),
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        # op_kwargs={'age':18}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )
    [task2, task3] >> task1