from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def get_sklearn():
    # import sklearn
    # print(f'scikit-learn version: {sklearn.__version__}')
    print("get_sklearn")    

default_args = {
    'owner': 'airflow',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_python_dependencies_v01',
    default_args=default_args,
    start_date=datetime(2023,10,11),
    schedule_interval='@daily',
) as dag:
    getsklearn=PythonOperator(
        task_id='get_sklearn',
        python_callable=get_sklearn,
    )

    getsklearn