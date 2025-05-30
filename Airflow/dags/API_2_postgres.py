from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 28),
    'retries': 2
}

dag = DAG(
    'API_2_postgres',
    default_args=default_args,
    description='Download data from different API and upload to postgres',
    schedule_interval=None,  # manual run
    catchup=False
)


spotify_data = BashOperator(
    task_id='spotify_data',
    bash_command=f'python "{os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'dags/tasks/spotify/spotify_data.py')}"',
    dag=dag
)

openweathermap_data = BashOperator(
    task_id='openweathermap_data',
    bash_command=f'python "{os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'dags/tasks/openweathermap/openweathermap_data.py')}"',
    dag=dag
)

spotify_data >> openweathermap_data
