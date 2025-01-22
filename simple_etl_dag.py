from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Funkcje symulujące kroki ETL
def extract():
    print("Extracting data...")
    return {"data": [1, 2, 3, 4, 5]}

def transform(ti):
    data = ti.xcom_pull(task_ids='extract_task')
    transformed_data = [x * 2 for x in data['data']]
    print(f"Transformed data: {transformed_data}")
    return transformed_data

def load(ti):
    transformed_data = ti.xcom_pull(task_ids='transform_task')
    print(f"Loading data: {transformed_data}")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definicja DAG
dag = DAG(
    'simple_etl_dag',
    default_args=default_args,
    description='A simple ETL process DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Zadania
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    dag=dag,
)

# Zależności między zadaniami
extract_task >> transform_task >> load_task
