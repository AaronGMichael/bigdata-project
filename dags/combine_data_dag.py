from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.combine_data import combine_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def combine_data_task(**kwargs):
    combine_data()

with DAG(
    'combine_data_dag',
    default_args=default_args,
    description='Combine DataSources',
    schedule_interval=None,
    catchup=False,
) as dag:

    convert = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data_task,
        provide_context=True,
    )

