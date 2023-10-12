from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_postgres_operator_v03',
    default_args=default_args,
    start_date=datetime(2023,10,11),
    schedule_interval='0 0 * * *',
) as dag:
    task1 = PostgresOperator(
        task_id='task1',
        postgres_conn_id='postgres_conn',
        sql="""CREATE TABLE IF NOT EXISTS dag_runs(
            dt date,
            dag_id character varying,
            primary key(dt, dag_id)
        )"""
    )

    task2 = PostgresOperator(
        task_id='task2',
        postgres_conn_id='postgres_conn',
        sql="""insert into dag_runs(dt, dag_id) values ('{{ ds }}' , '{{ dag.dag_id }}')"""
    )

    task3 = PostgresOperator(
        task_id='task3',
        postgres_conn_id='postgres_conn',
        sql="""delete from dag_runs where dt='{{ ds }}' and dag_id = '{{ dag.dag_id }}';"""
    )

    task1 >> task3 >> task2