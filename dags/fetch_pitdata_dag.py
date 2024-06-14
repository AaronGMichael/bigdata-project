# fetch_pitdata_dag.py

import os
import re
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# os.environ["HADOOP_HOME"] = "E:\\Apps\\hadoop-2.8.3"

cwd = os.getcwd()  # Get the current working directory (cwd)
cwd = cwd.replace("\\", "/")
cwd = cwd + "/../datalake/"
DATALAKE_ROOT_FOLDER = cwd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from ..lib.get_data import fetch_pitdata_from_db

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'fetch_pitdata_dag',
    default_args=default_args,
    description='Fetch pit data from DB',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    fetch_pitdata = PythonOperator(
        task_id='fetch_pitdata',
        python_callable=fetch_pitdata_from_db,
    )
