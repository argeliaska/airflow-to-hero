from datetime import datetime, timedelta
import pendulum
import logging
import csv

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args={
    'owner': 'argeliaska',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def postgres_to_s3(ds_nodash, next_ds_nodash):
    # step 1: query data from postgresql db an save into text file
    hook = PostgresHook(postgres_conn_id='psql_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()
    # cursor.execute(f"select * from orders where created_at <= '20220501'")
    cursor.execute(f"select * from orders where created_at >= '{ds_nodash}' and created_at < '{next_ds_nodash}'")
    with open(f"dags/get_orders_{ds_nodash}.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info(f"Saved orders data in text file dags/get_orders_{ds_nodash}.txt")
        
    # step 2: upload text file into S3
    bucket_name = Variable.get("AWS_BUCKET_NAME")
    s3hook = S3Hook(aws_conn_id="aws_conn")
    s3hook.load_file(
        filename=f"dags/get_orders_{ds_nodash}.txt",
        key=f"orders/{ds_nodash}.txt",
        bucket_name=bucket_name,
        replace=True,
    )
    logging.info(f"Saved file orders/{ds_nodash}.txt in S3 bucket {bucket_name}")



with DAG(
    dag_id='dag_with_postgress_hook_v03',
    default_args=default_args,
    start_date=pendulum.datetime(2022, 5, 1, tz='UTC'),
    end_date=pendulum.datetime(2022, 5, 5, tz='UTC'),
    schedule_interval='@daily',
) as dag: 
    task1 = PythonOperator(
        task_id="postres_to_s3",
        python_callable=postgres_to_s3,
    )
    task1