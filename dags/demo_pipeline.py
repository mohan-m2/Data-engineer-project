# filepath: /usr/local/airflow/dags/demo_pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure Airflow finds the script correctly
sys.path.append('/usr/local/airflow/scripts')

try:
    from demo import my_task
except ImportError as e:
    print(f"Error importing my_task: {e}")
    raise

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'demo_pipeline',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),  # Corrected parameter name
)

# Define the task
run_my_task = PythonOperator(
    task_id='run_my_task',
    python_callable=my_task,
    dag=dag,
)

# Set task dependencies (optional, as there’s only one task)
run_my_task
