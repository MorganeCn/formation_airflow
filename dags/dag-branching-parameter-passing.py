import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import timedelta
from pendulum import datetime
import random


def process_data(
    ti # task_instance
):
    # Lire les données depuis le fichier CSV téléchargé
    df = pd.read_csv('/tmp/flowerdataset.csv')

    # Calcul de la somme sepal_length + sepal_width
    df["somme"] = df["sepal_length"] + df["sepal_width"]
    print(df.head())
    
    # Pour passer la longueur du DataFrame à la tâche de branching
    key = 'longueur'
    value = len(df)
    print(value)
    ti.xcom_push(key, value)


def branch_by_option(ti):
    value = ti.xcom_pull(task_ids='process_data', key='longueur')
    if value > 1000:
        return 'large_dataset_branch'
    else:
        return 'small_dataset_branch'


def print_message(message):
    print(message)


# Définir les arguments par défaut du DAG
default_args = {
    'owner': 'bsp',
    'start_date': datetime(2024, 3, 25),
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

# Instancier le DAG
dag = DAG(
    'dag-branching-parameter-passing',
    default_args=default_args,
    schedule_interval='@daily',
)

# Tâche pour télécharger les données
download_data_task = BashOperator(
    task_id='download_data',
    bash_command='curl -o /tmp/flowerdataset.csv https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv',
    dag=dag
)

# Tâche pour traiter les données et les stocker dans la base de données
process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    #provide_context=True,
    dag=dag,
)

# Tâche pour la branching
branching_task = BranchPythonOperator(
    task_id='branching',
    python_callable=branch_by_option,
    dag=dag,
)

# Tâche pour un dataset large
large_dataset_task = PythonOperator(
    task_id='large_dataset_branch',
    python_callable=print_message,
    op_kwargs={'message': 'Gros dataset'},
    dag=dag,
)

# Tâche pour un dataset petit
small_dataset_task = PythonOperator(
    task_id='small_dataset_branch',
    python_callable=print_message,
    op_kwargs={'message': 'Petit dataset'},
    dag=dag,
)

# Définir les dépendances entre les tâches
download_data_task >> process_data_task >> branching_task >> [large_dataset_task, small_dataset_task]