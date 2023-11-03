from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable
import json
import requests
import pandas as pd
import os
import logging

logger = logging.getLogger(_name_)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


def file_size_validation(**kwargs):
    try:
        csv_file_path = '/home/bipin/FInal-Project-/Data/Raw_Data/googleplaystore.csv'

        file_size_byte = os.path.getsize(csv_file_path)

        file_size_kb = file_size_byte / 1024

        if 0 < file_size_kb <= 2000:
            logger.info("File Size Validation is successful")

        
    
    except Exception as e:
        logger.error("File size Validation failed: %s", str(e))


# def validate_column(**kwargs):
#     csv_file_path = '/home/bipin/FInal-Project-/Data/Raw_Data/googleplaystore.csv'

#     expected_column_name = ['']

with DAG(
    dag_id = "airflow_project_testing",
    schedule_interval='@daily',
    start_date=datetime(2023,11,1),
    catchup=False
) as dag:

    File_Size_Validation = PythonOperator(
        task_id='file_size_validation',
        python_callable=file_size_validation,
        provide_context=True,
    )

     
    spark_submit = BashOperator(
        task_id='spark_submit_task',
        bash_command="spark-submit /home/bipin/FInal-Project-/Saving_Raw_Data_To_Db.py",
    )
    
    File_Size_Validation >> spark_submit