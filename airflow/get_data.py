from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
sys.path.insert(0, '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/app')
import fetch_data

default_args = {
    'owner': 'CodeOTS',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='dag_fetch_crypto_data',
    default_args=default_args, 
    schedule='0 0 * * *', 
    catchup=False, 
    start_date=datetime(2024, 1, 1), 
    tags=['crypto'],
    )

#this function defines your DAG 
def dag_fetch_crypto_data():
    @task
    def task_fetch_data():
        payload = fetch_data.create_payload('d1', fetch_data.start_timestamp, fetch_data.end_timestamp)
        raw_data, call_timestamp = fetch_data.fetch_data(payload, fetch_data.cc_history_url, fetch_data.headers)
        # return fetch_data.fetch_data(payload, fetch_data.cc_history_url, fetch_data.headers)
        return {'raw_data': raw_data, 'call_timestamp': call_timestamp}

    @task
    def task_process_data(data):
        processed_data = fetch_data.add_call_timestamp_to_data(data['raw_data'])
        # return {'processed_data': processed_data, 'call_timestamp': data['call_timestamp']}
        return {'processed_data': processed_data, 'call_timestamp': data['call_timestamp']}

    @task    
    def task_save_data(data):
        fetch_data.savejson_hdfs(raw_data=data['processed_data'], 
                            call_timestamp=data['call_timestamp'])
        print('Finished saving data!')

    fetched_data = task_fetch_data()
    processed_data = task_process_data(fetched_data)
    task_save_data(processed_data)

execute_dag = dag_fetch_crypto_data()