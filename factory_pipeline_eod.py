from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Folder holding the pipeline scripts and factory_dbt (defaults to this DAG's folder)
PROJECT_DIR = os.getenv('FACTORY_PROJECT_DIR', os.path.dirname(os.path.abspath(__file__)))

default_args = {
    'owner': 'data-eng',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'factory_maintenance_eod',
    default_args=default_args,
    description='EOD: Run dbt models and send maintenance report',
    schedule='0 23 * * *',  # Daily at 23:00
    catchup=False,
    tags=['factory', 'eod', 'reporting']
)

def send_report():
    """Import and run report generator"""
    from send_report import main
    main()

task_dbt = BashOperator(
    task_id='run_dbt', # ← dbt connects to Snowflake and runs all models to refresh data for the report
    bash_command=f'cd "{PROJECT_DIR}/factory_dbt" && dbt run',
    dag=dag
)

task_email = PythonOperator(
    task_id='send_email_report',
    python_callable=send_report,
    dag=dag
)

task_dbt >> task_email
