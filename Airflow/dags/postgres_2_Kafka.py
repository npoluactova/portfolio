from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 28),
    'retries': 2
}

dag = DAG(
    'postgres_2_Kafka',
    default_args=default_args,
    description='Upload data from postgres and send to Kafka and read from kafka and write to postgres',
    schedule_interval='@daily',  # manual run
    catchup=False
)

# Sensor that awaits finishing of task "spotify_data" from Dag "API_2_postgres"
spotify_data_sensor = ExternalTaskSensor(
    task_id='spotify_data_sensor',
    external_dag_id='API_2_postgres',  # name of dag that is awaited
    external_task_id='spotify_data',  # name of task that is awaited from external dag
    poke_interval=30,  # check frequency = every 30 sec
    timeout=600,  # max waiting time = 600 sec = 10 min
    mode='poke',  # waiting mode (poke = constant check)
    dag=dag
)

spotify_kafka_producer = BashOperator(
    task_id='spotify_kafka_producer',
    bash_command=f'python "{os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'dags/tasks/kafka/producer/spotify_kafka_producer.py')}"',
    dag=dag
)

spotify_data_sensor >> spotify_kafka_producer
