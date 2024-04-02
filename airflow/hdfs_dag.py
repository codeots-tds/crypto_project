from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import subprocess
import sys

sys.path.append('/home/ra-terminal/Desktop/portfolio_projects/crypto_project/app/data')
import load_data_to_db

default_args = {
    'owner': 'CodeOTS',
    'depends_on_past': False,
    'start_date': datetime(2024,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

@dag(
    dag_id='dag_hdfs_processing',
    default_args=default_args,
    schedule_interval='15 0 * * *',
    tags=['crypto'],
)
def dag_process_crypto_data():

    wait_for_fetch_crypto_data = ExternalTaskSensor(
        task_id='wait_for_fetch_crypto_data',
        external_dag_id='dag_fetch_crypto_data',
        external_task_id='task_save_data',
        timeout=1800,
        poke_interval=60, # How often to check for the condition
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule'
    )
    
    def run_script(script_path):
        subprocess.run(["bash", script_path], check=True)

    @task
    def serialize_json_data():
        run_script('/home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/convert_json.sh')

    wait_for_fetch_crypto_data >> serialize_json_data()
dag_instance = dag_process_crypto_data()
# default_args = {
#     'owner': 'CodeOTS',
#     'start_date': datetime(2023, 1, 1),
#     'retries': 3,
#     'retry_delay': timedelta(minutes=5),
# }

# def run_script(script_path):
#     subprocess.run(["bash", script_path], check=True)

# with DAG(
#     'dag_hdfs_processing',
#     default_args=default_args,
#     schedule_interval='15 0 * * *',
#     catchup=False,
#     tags=['crypto'],
# ) as dag:

#     wait_for_get_data = ExternalTaskSensor(
#     task_id='wait_for_get_data',
#     external_dag_id='dag_fetch_crypto_data',
#     external_task_id='task_save_data',
#     check_existence=True,
#     execution_delta=timedelta(seconds=10),  # Adjust this based on actual timing between tasks
#     timeout=600,  # seconds for timeout to wait for the task before retry
#     mode='reschedule',
#     poke_interval=15,
#     )

#     upload_datafiles_to_hadoop = PythonOperator(
#         task_id='upload_datafiles_to_hadoop',
#         python_callable=run_script,
#         op_kwargs={'script_path': '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/upload_to_hadoop.sh'},
#     )
    
#     run_hadoop_mapper_job = PythonOperator(
#         task_id='run_hadoop_job',
#         python_callable=run_script,
#         op_kwargs={'script_path': '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/run_hadoop_job.sh'},
#     )

#     download_datafiles_to_local = PythonOperator(
#         task_id='download_datafiles_to_local',
#         python_callable=run_script,
#         op_kwargs={'script_path': '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/download_to_local.sh'},
#     )

#     serialize_json_files = PythonOperator(
#         task_id='serialize_json_files',
#         python_callable=run_script,
#         op_kwargs={'script_path': '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/convert_json.sh'},
#     )

#     delete_datafiles_from_local = PythonOperator(
#         task_id='delete_datafiles_from_local',
#         python_callable=run_script,
#         op_kwargs={'script_path': '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/delete_datafiles.sh'},
#     )


#     def save_data_to_pgdb_task_fn():
#         load_data_to_db.process_directory(load_data_to_db.conn, load_data_to_db.cur, load_data_to_db.tablename, load_data_to_db.directory)
#         load_data_to_db.conn.commit()
#         load_data_to_db.cur.close()
#         load_data_to_db.conn.close()

#     save_data_to_pgdb_task = PythonOperator(
#     task_id='save_data_to_pgdb_task',
#     python_callable=save_data_to_pgdb_task_fn,
# )

#     wait_for_get_data >> serialize_json_files >> upload_datafiles_to_hadoop >> run_hadoop_mapper_job >> \
#     download_datafiles_to_local >> save_data_to_pgdb_task >> delete_datafiles_from_local

# from airflow import DAG
# from airflow.decorators import dag, task
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from airflow.sensors.external_task_sensor import ExternalTaskSensor

# import subprocess
# import sys
# # sys.path.insert(0, '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/hadoop/jobs')
# # import mapper
# sys.path.append('/home/ra-terminal/Desktop/portfolio_projects/crypto_project/app/data')
# import load_data_to_db

# default_args = {
#     'owner': 'CodeOTS',
#     'start_date': datetime(2023, 1, 1),
#     'retries': 3,
#     'retry_delay': timedelta(minutes=5)
# }

# @dag(
#     dag_id='dag_hdfs_processing',
#     default_args=default_args, 
#     schedule='15 0 * * *', 
#     catchup=False, 
#     start_date=datetime(2024, 1, 1),
#     description='A DAG for preprocessing the data files before loading it to postgres',
#     tags=['crypto'],
#     )

# def dag_hdfs_processing():
#     wait_for_get_data = ExternalTaskSensor(
#         task_id='wait_for_get_data',
#         external_dag_id='dag_fetch_crypto_data', # Use the actual DAG ID of get_data.py
#         external_task_id='task_save_data',  # Use a specific task ID or None to wait for the whole DAG
#         check_existence=True,
#         # execution_delta=timedelta(minutes=5),
#         timeout=600, #seconds for timeout to wait for the task before retry
#         mode='reschedule', #
#         poke_interval=15,
#     )

#     def run_script(script_path):
#         subprocess.run(["bash"], script_path, check=True)

#     #using the 'r' prefix tells Airflow to ignore the escape characters like \t, \n and treats as raw string.
#     upload_datafiles_to_hadoop = BashOperator(
#         task_id = 'upload_datafiles_to_hadoop',
#         python_callable=run_script,
#         op_kwargs={"script_path": "/home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/upload_to_hadoop.sh"},
#         # bash_command= r'bash /home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/upload_to_hadoop.sh'
#     )
    
#     run_hadoop_mapper_job = BashOperator(
#         task_id = 'run_hadoop_job',
#         python_callable=run_script,
#         op_kwargs={'script_path': '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/run_hadoop_job.sh'},
#         # bash_command= r'bash /home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/run_hadoop_job.sh'
#         # running with python instead
#         # bash_command='python /home/ra-terminal/Desktop/portfolio_projects/crypto_project/hadoop/jobs/mapper.py'
#     )

#     download_datafiles_to_local = BashOperator(
#         task_id='download_datafiles_to_local',
#         python_callable=run_script,
#         op_kwargs={'script_path': '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/download_to_local.sh'},
#         #bash_command= r'bash /home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/download_to_local.sh'
#     )

#     serialize_json_files=BashOperator(
#         task_id='serialize_json_files',
#         python_callable=run_script,
#         op_kwargs={'script_path': '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/convert_json.sh'},
#         # bash_command= r'bash /home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/convert_json.sh'
#     )

#     delete_datafiles_from_local=BashOperator(
#         task_id='delete_datafiles_from_local',
#         python_callable=run_script,
#         op_kwargs={'script_path': '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/delete_datafiles.sh'},
#         # bash_command='bash /home/ra-terminal/Desktop/portfolio_projects/crypto_project/scripts/delete_datafiles.sh'
#     )

#     @task
#     def save_data_to_pgdb_task():
#         load_data_to_db.process_directory(load_data_to_db.conn, 
#                                     load_data_to_db.cur, 
#                                     load_data_to_db.tablename, 
#                                     load_data_to_db.directory)
#         load_data_to_db.conn.commit()
#         load_data_to_db.cur.close()
#         load_data_to_db.conn.close()

#     wait_for_get_data >> serialize_json_files >> upload_datafiles_to_hadoop >> run_hadoop_mapper_job >> \
#     download_datafiles_to_local >> save_data_to_pgdb_task() >> delete_datafiles_from_local

# dag = dag_hdfs_processing()