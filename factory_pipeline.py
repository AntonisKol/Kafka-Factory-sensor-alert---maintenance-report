from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Folder holding the pipeline scripts (defaults to this DAG's folder)
PROJECT_DIR = os.getenv('FACTORY_PROJECT_DIR', os.path.dirname(os.path.abspath(__file__)))

default_args = {
    'owner': 'data-eng',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'factory_maintenance_pipeline',
    default_args=default_args,
    description='Every 15 minutes: poll Kafka, flag anomalies with Spark, load to Snowflake',
    schedule='*/15 * * * *',
    catchup=False,
    max_active_runs=1, # <- each step picks the latest /tmp file, so runs must not overlap
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

def load_to_snowflake():
    from snowflake_loader import main
    main()

task_poll = PythonOperator(
    task_id='poll_kafka_to_json',
    python_callable=poll_kafka,
    dag=dag
)

task_spark = BashOperator(
    task_id='spark_anomaly_detection', # ← reads the latest raw JSON, writes flagged Parquet
    bash_command=f'cd "{PROJECT_DIR}" && python spark_anomaly_detection.py',
    dag=dag
)

task_load = PythonOperator(
    task_id='load_to_snowflake', # ← loads raw readings + alerts into Snowflake
    python_callable=load_to_snowflake,
    dag=dag
)

task_poll >> task_spark >> task_load
