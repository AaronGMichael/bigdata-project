from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.make_elastic_indexes import make_elastic_indexes

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def elastic_index_task(**kwargs):
    make_elastic_indexes()

with DAG(
    'send_to_elasticsearch_dag',
    default_args=default_args,
    description='Send Data to Elasticsearch',
    schedule_interval=None,
    catchup=False,
) as dag:

    convert = PythonOperator(
        task_id='send_data_to_elasticsearch',
        python_callable=elastic_index_task,
        provide_context=True,
    )

