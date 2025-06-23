from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
import random

# Fonction pour extraire le prix historique du Bitcoin pour 10 jours
def extract_bitcoin_price(**kwargs):
    # Simuler des données pour 10 jours
    simulated_data = []
    for i in range(10):
        simulated_data.append({
            'date': (datetime.now() - timedelta(days=i)).strftime('%d-%m-%Y'),
            'current_price': {'usd': random.uniform(30000, 60000)},
            'total_volume': {'usd': random.uniform(1000000, 10000000)}
        })
    return simulated_data

# Fonction pour calculer le RSI
def calculate_rsi(prices):
    gains = []
    losses = []

    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            losses.append(-change)
            gains.append(0)

    avg_gain = sum(gains) / len(gains) if gains else 0
    avg_loss = sum(losses) / len(losses) if losses else 0

    if avg_loss == 0:
        rsi = 100
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    return rsi

# Fonction pour traiter les données
def process_data(ti):
    market_data = ti.xcom_pull(task_ids='extract_bitcoin_price')

    # Extraire les prix en USD pour le calcul du RSI
    prices = [day['current_price']['usd'] for day in market_data]

    # Calculer le RSI
    rsi = calculate_rsi(prices)

    # Récupérer les données du jour d'exécution
    execution_day_data = market_data[0]  # Le premier élément est le jour d'exécution
    processed_data = {
        'prix en usd': execution_day_data['current_price']['usd'],
        'volume en usd': execution_day_data['total_volume']['usd'],
        'RSI': rsi
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
    'bitcoin_price_historical_rsi_dag',
    default_args=default_args,
    description='Un DAG pour extraire, traiter et calculer le RSI du prix historique du Bitcoin',
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
