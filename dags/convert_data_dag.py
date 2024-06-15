from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.convert_data import convert_data_from_root
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}


def convert_data_task(**kwargs):
    conf = kwargs.get('dag_run').conf
    data = conf.get('data')
    print("!!!!!!!" + data)
    convert_data_from_root(data)


with DAG(
    'convert_data_dag',
    default_args=default_args,
    description='Convert data',
    schedule_interval=None,  # This will be triggered by the fetch DAGs
    catchup=False,
) as dag:

    convert = PythonOperator(
        task_id='convert_data',
        python_callable=convert_data_task,
        provide_context=True,
    )

