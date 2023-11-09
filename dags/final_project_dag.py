from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import pandas as pd
import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

Data_Path = Variable.get("csv_file_path")
extraction_script = Variable.get("extraction_script")
transformation_script = Variable.get("Transformation_Script")
Business_metrics = Variable.get("Business_metrics_script")

def process_csv_files():

    input_folder = Variable.get("input_folder")
    output_folder = Variable.get("output_folder")

    columns_to_check = ["App", "Category", "Rating", "Reviews", "Size", "Installs",
                    "Type", "Price", "Content Rating", "Genres", "Last Updated",
                    "Current Ver", "Android Ver"]
    
    csv_files = [filename for filename in os.listdir(input_folder) if filename.endswith('.csv')]

    for filename in csv_files:
        file_path = os.path.join(input_folder, filename)
        try:
            df = pd.read_csv(file_path)
            
            if all(column in df.columns for column in columns_to_check) and len(df.columns) == len(columns_to_check):
                output_file_path = os.path.join(output_folder, filename)
                df.to_csv(output_file_path, index=False)

        except pd.errors.EmptyDataError:
            pass


def file_size_validation(output_folder, **kwargs):
    try:
        for filename in os.listdir(output_folder):
            file_path = os.path.join(output_folder, filename)

            file_size_byte = os.path.getsize(file_path)
            file_size_kb = file_size_byte / 1024

            if 0 < file_size_kb <= 2000:
                logger.info("File Size Validation is successful for file: %s", filename)
            else:
                logger.warning("File Size Validation failed for file: %s", filename)
 
    except Exception as e:
        logger.error("File size Validation failed: %s", str(e))


with DAG(
    dag_id = "airflow_project_testing",
    schedule_interval='@daily',
    start_date=datetime(2023,11,8),
    catchup=False
) as dag:

    File_type_validation = PythonOperator(
        task_id='file_validation',
        python_callable=process_csv_files,
        provide_context=True,
    )

    File_Size_Validation = PythonOperator(
        task_id='file_size_validation',
        python_callable=file_size_validation,
        op_args=['/home/bipin/FInal-Project-/Data/Raw_Data'],
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

    Business_metrics = BashOperator(
        task_id = 'Business_Metrics_Generation',
        bash_command = "spark-submit {{var.value.Business_metrics_script}}"
    )

    
    File_type_validation >>  File_Size_Validation >> Raw_Data_To_Db >> Transform_And_Load >> Business_metrics
