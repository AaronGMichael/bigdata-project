from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'example_http_operator',
    default_args=default_args,
    description='A simple HTTP DAG',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Check if the API is available
    check_api_availability = HttpSensor(
        task_id='check_api_availability',
        http_conn_id='my_http_connection',
        endpoint='/api/v1/resource',
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    )

    # Task to trigger the API call
    trigger_api_call = SimpleHttpOperator(
        task_id='trigger_api_call',
        http_conn_id='jsonplaceholder_connection',  # If using connection
        endpoint='todos/1',
        method='GET',
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )

    check_api_availability >> trigger_api_call
