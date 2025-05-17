from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 19),
    'retries': 2
}

dag = DAG(
    'bq_2_google_drive',
    default_args=default_args,
    description='Download data from BQ and upload to Google Drive',
    schedule_interval=None,  # manual run
    catchup=False
)


city_bikes_data = BashOperator(
    task_id='city_bikes_data',
    bash_command=f'python "{os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'dags/tasks/city_bikes/city_bikes_data.py')}"',
    dag=dag
)

city_bikes_data
