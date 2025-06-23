from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import logging

# Fonction pour extraire le prix du Bitcoin
def extract_bitcoin_price(**kwargs):
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
    response = requests.get(url)
    data = response.json()
    bitcoin_data = data['bitcoin']
    return bitcoin_data

# Fonction pour traiter les données
def process_data(ti):
    bitcoin_data = ti.xcom_pull(task_ids='extract_bitcoin_price')
    processed_data = {
        'usd': bitcoin_data['usd'],
        'change': bitcoin_data['usd_24h_change']
    }
    return processed_data

# Fonction pour stocker les données (simuler l'enregistrement)
def store_data(ti):
    processed_data = ti.xcom_pull(task_ids='process_data')
    logging.info(f"Storing data: {processed_data}")

# Définition du DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

dag = DAG(
    'bitcoin_price_dag',
    default_args=default_args,
    description='Un DAG pour extraire et traiter le prix du Bitcoin',
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
