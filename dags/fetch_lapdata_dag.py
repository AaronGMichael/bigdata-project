from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from lib import get_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def trigger_convert_data_lap(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_lapdata', key='return_value')
    return {'data': data}


def fetch_lapdata_from_db(**kwargs):
    output = get_data.fetch_lapdata_from_db()
    ti = kwargs['ti']
    ti.xcom_push(key='return_value', value=output)
    return output


with DAG(
    'fetch_lapdata_dag',
    default_args=default_args,
    description='Fetch lap data from DB',
    schedule_interval=None,
    catchup=False,
) as dag:

    fetch_lapdata = PythonOperator(
        task_id='fetch_lapdata',
        python_callable=fetch_lapdata_from_db,
        provide_context=True,
    )

    trigger_conversion_dag = TriggerDagRunOperator(
        task_id='trigger_conversion_dag',
        trigger_dag_id='convert_data_dag',
        conf={"data": "lapData"},
        wait_for_completion=False,
    )

fetch_lapdata >> trigger_conversion_dag

