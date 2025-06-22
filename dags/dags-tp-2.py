from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import logging

# Fonction pour extraire le prix historique du Bitcoin
def extract_bitcoin_price(**kwargs):
    execution_date = kwargs['execution_date']
    formatted_date = execution_date.strftime('%d-%m-%Y')  # Format de la date
    url = f"https://api.coingecko.com/api/v3/coins/bitcoin/history?date={formatted_date}&localization=false"
    response = requests.get(url)
    data = response.json()
    market_data = data['market_data']
    return market_data

# Fonction pour traiter les données
def process_data(**kwargs):
    ti = kwargs['ti']
    market_data = ti.xcom_pull(task_ids='extract_bitcoin_price')
    processed_data = {
        'prix en usd': market_data['current_price']['usd'],
        'volume en usd': market_data['total_volume']['usd']
    }
    return processed_data

# Fonction pour stocker les données (simuler l'enregistrement)
def store_data(**kwargs):
    ti = kwargs['ti']
    processed_data = ti.xcom_pull(task_ids='process_data')
    logging.info(f"Storing data: {processed_data}")

# Définition du DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

dag = DAG(
    'bitcoin_price_historical_dag',
    default_args=default_args,
    description='Un DAG pour extraire et traiter le prix historique du Bitcoin',
    schedule_interval='@daily',
)

# Définition des tâches
extract_task = PythonOperator(
    task_id='extract_bitcoin_price',
    python_callable=extract_bitcoin_price,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

store_task = PythonOperator(
    task_id='store_data',
    python_callable=store_data,
    provide_context=True,
    dag=dag,
)

# Définition de l'ordre des tâches
extract_task >> process_task >> store_task
