# fetch_lapdata_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.get_data import fetch_lapdata_from_db



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'fetch_lapdata_dag',
    default_args=default_args,
    description='Fetch lap data from DB',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    fetch_lapdata = PythonOperator(
        task_id='fetch_lapdata',
        python_callable=fetch_lapdata_from_db,
    )
