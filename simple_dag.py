from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    print("Hello, World!")
    return 'Hello, World!'

def print_goodbye():
    print("Goodbye, World!")
    return 'Goodbye, World!'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Define tasks
task_start = DummyOperator(
    task_id='start',
    dag=dag
)

task_hello = PythonOperator(
    task_id='say_hello',
    python_callable=print_hello,
    dag=dag
)

task_goodbye = PythonOperator(
    task_id='say_goodbye',
    python_callable=print_goodbye,
    dag=dag
)

task_end = DummyOperator(
    task_id='end',
    dag=dag
)

# Set up task dependencies
task_start >> task_hello >> task_goodbye >> task_end
