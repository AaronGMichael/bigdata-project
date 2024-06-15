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

def trigger_convert_data_pit(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_pitdata', key='return_value')
    return {'data': data}


with DAG(
    'fetch_pitdata_dag',
    default_args=default_args,
    description='Fetch pit data from DB',
    schedule_interval=None,
    catchup=False,
) as dag:

    fetch_pitdata = PythonOperator(
        task_id='fetch_pitdata',
        python_callable=get_data.fetch_pitdata_from_db,
        provide_context=True,
    )

    trigger_conversion_dag = TriggerDagRunOperator(
        task_id='trigger_conversion_dag',
        trigger_dag_id='convert_data_dag',
        conf={"data": "pitData"},
        wait_for_completion=False,
    )

    fetch_pitdata >> trigger_conversion_dag
