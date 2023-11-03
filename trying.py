from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.filesystem import FileSensor
import pandas as pd
import pyarrow as pa
from datetime import datetime, timedelta
import requests
import csv
import os


default_args = {
    'owner': 'Teamproject',
    'start_date': datetime(2023, 9, 18),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_pg = DAG(
    'my_project_dag',
    default_args=default_args,
    description='Your DAG description',
    schedule_interval=timedelta(days=1),
    start_date= default_args['start_date'],
    catchup=False, 
)

def hello_world():
    print("Hello, World!")

# Create a PythonOperator to execute the hello_world function
hello_task = PythonOperator(
    task_id='print_hello_world',
    python_callable=hello_world,
    dag=dag_pg
)

create_table_query = """
CREATE TABLE IF NOT EXISTS apps_raw_data_table(
    App STRING,
    Category STRING,
    Rating STRING,
    Reviews STRING,
    Size STRING,
    Installs STRING,
    Type STRING,
    Price STRING,
    Content_Rating STRING,
    Genres STRING,
    Last_Updated STRING,
    Current_Ver STRING,
    Android_Ver STRING
);
"""
create_table_task = PostgresOperator(
    task_id='create_app_table',
    sql=create_table_query,
    postgres_conn_id='postgres_local',
    dag=dag_pg
)


# Set up dependencies
hello_task >> create_table_task

