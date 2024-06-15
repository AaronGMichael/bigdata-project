from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'trigger_job',
    default_args=default_args,
    description='Trigger pipeline for f1 data',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    trigger_fetch_lapdata_dag = TriggerDagRunOperator(
        task_id='trigger_fetch_lapdata_dag',
        trigger_dag_id='fetch_lapdata_dag',
        wait_for_completion=False,
    )

    trigger_fetch_pitdata_dag = TriggerDagRunOperator(
        task_id='trigger_fetch_pitdata_dag',
        trigger_dag_id='fetch_pitdata_dag',
        wait_for_completion=False,
    )

    trigger_combine_data_dag = TriggerDagRunOperator(
        task_id='trigger_combine_data_dag',
        trigger_dag_id='combine_data_dag',
        wait_for_completion=False,
    )

    trigger_send_to_elasticsearch_dag = TriggerDagRunOperator(
        task_id='trigger_send_to_elasticsearch_dag',
        trigger_dag_id='send_to_elasticsearch_dag',
        wait_for_completion=False,
    )

trigger_fetch_lapdata_dag >> trigger_fetch_pitdata_dag >> trigger_combine_data_dag >> trigger_send_to_elasticsearch_dag


