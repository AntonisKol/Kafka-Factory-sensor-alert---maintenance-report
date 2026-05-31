"""
File: dags/factory_pipeline.py

Airflow DAG: Factory Sensor Maintenance Pipeline
Schedule: Every 15 minutes
Task: Python consumer polls Kafka, saves batch to JSON files
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-eng',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'factory_maintenance_pipeline',
    default_args=default_args,
    description='Poll Kafka every 15 minutes, save batch to JSON files',
    schedule='*/15 * * * *',
    catchup=False,
    tags=['factory', 'sensors']
)

def poll_kafka():
    from consumer import poll_kafka_to_json
    
    result = poll_kafka_to_json()
    
    if result:
        print(f"✓ Polled {result['record_count']} messages")
        print(f"  JSON: {result['json_path']}")
        return result
    else:
        raise Exception("Consumer returned no data")

task = PythonOperator(
    task_id='poll_kafka_to_json',
    python_callable=poll_kafka,
    dag=dag
)
