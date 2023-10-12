from datetime import datetime, timedelta

from airflow.decorators import dag, task


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='dag_with_taskflow_api_v01',
     default_args=default_args,
     start_date=datetime(2023,10,11),
    schedule_interval='@daily')
def hello_world_etl():

    @task()
    def get_name():
        return "Jerry"
    
    @task()
    def get_age():
        return 20

    @task()
    def greet(name, age):
        print(f"Hello World! My name is {name} and I am {age} years old")

    name = get_name()
    age = get_age()
    greet(name, age)

greet_dag = hello_world_etl()