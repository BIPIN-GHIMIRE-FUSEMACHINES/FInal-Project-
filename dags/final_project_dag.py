from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable
import pandas as pd
import os
import logging
import pandas as pd

logger = logging.getLogger(_name_)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

Data_Path = Variable.get("csv_file_path")
extraction_script = Variable.get("extraction_script")
transformation_script = Variable.get("Transformation_Script")

def file_type_validation(**kwargs):
    try:
        csv_file_path = Data_Path
        if csv_file_path.endswith('.csv'):
            logger.info("File Type Validation is successful")
            return True
        else:
            logger.error("File Type Validation failed: Not a CSV file")
            return False
    except Exception as e:
        logger.error("File Type Validation failed: %s", str(e))
        return False

def file_size_validation(**kwargs):
    try:
        csv_file_path = Data_Path

        file_size_byte = os.path.getsize(csv_file_path)

        file_size_kb = file_size_byte / 1024

        if 0 < file_size_kb <= 2000:
            logger.info("File Size Validation is successful")
 
    except Exception as e:
        logger.error("File size Validation failed: %s", str(e))

def column_header_validation(**kwargs):
    try:
        required_columns = [
            "App", "Category", "Rating", "Reviews", "Size",
            "Installs", "Type", "Price", "Content Rating",
            "Genres", "Last Updated", "Current Ver", "Android Ver"
        ]
        
        csv_file_path = Data_Path
        df = pd.read_csv(csv_file_path)
        columns = df.columns.tolist()
        
        if all(col in columns for col in required_columns):
            logger.info("Column Validation is successful")
            return True
        else:
            missing_columns = [col for col in required_columns if col not in columns]
            logger.error("Column Validation failed: Missing columns - %s", missing_columns)
            return False
    except Exception as e:
        logger.error("Column Validation failed: %s", str(e))
        return False
    
with DAG(
    dag_id = "airflow_project_testing",
    schedule_interval='@daily',
    start_date=datetime(2023,11,1),
    catchup=False
) as dag:

    File_type_validation = PythonOperator(
        task_id='file_extention_validation',
        python_callable=file_type_validation,
        provide_context=True,
    )

    File_Size_Validation = PythonOperator(
        task_id='file_size_validation',
        python_callable=file_size_validation,
        provide_context=True,
    )

    column_header_validation = PythonOperator(
        task_id='column_header_validation',
        python_callable=column_header_validation,
        provide_context=True,
    )
     
    Raw_Data_To_Db = BashOperator(
        task_id='Raw_Data_Dump',
        bash_command="spark-submit {{ var.value.extraction_script }}",
    )

    Transform_And_Load = BashOperator(
        task_id = 'Transform_And_Load',
        bash_command = "spark-submit {{ var.value.Transformation_Script }}"
    )

    
    File_type_validation >>  File_Size_Validation >> column_header_validation >> Raw_Data_To_Db >> Transform_And_Load