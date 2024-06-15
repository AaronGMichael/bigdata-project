# convert_data_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.convert_data import convert_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'convert_data_dag',
    default_args=default_args,
    description='Convert data',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    convert = PythonOperator(
        task_id='convert_data',
        python_callable=convert_data,
    )
