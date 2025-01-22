from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def fetch_eur_pln_rate():
    """Fetch the current EUR/PLN exchange rate from NBP API and log it."""
    url = "https://api.nbp.pl/api/exchangerates/rates/A/EUR?format=json"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses
        data = response.json()
        rate = data['rates'][0]['mid']
        print(f"Current EUR/PLN rate: {rate}")
        return rate
    except Exception as e:
        print(f"Error fetching EUR/PLN rate: {e}")
        raise

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
    'eur_pln_rate_dag',
    default_args=default_args,
    description='A DAG to fetch and log EUR/PLN exchange rate',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Define tasks
task_fetch_rate = PythonOperator(
    task_id='fetch_eur_pln_rate',
    python_callable=fetch_eur_pln_rate,
    dag=dag
)

# No dependencies needed for this simple DAG
