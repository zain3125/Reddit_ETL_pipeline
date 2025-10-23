from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime 
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.reddit_pipeline import extract_reddit_data, load_data_to_database
default_args = {
    'owner': 'zain',
    'start_date': datetime(2025, 10, 17),
    'retries': 1,}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='reddit_pipeline_dag',
    default_args=default_args,
    description='A DAG for Reddit data pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'pipeline'],
    )

# Extraction from reddit
extract_task = PythonOperator(
    task_id='extract_reddit_data',
    python_callable=extract_reddit_data,
    op_kwargs={
        'file_name': f'reddit_posts_{file_postfix}',
        'subreddits': ['Egypt', 'CAIRO', 'AlexandriaEgy', 'Masr'],
        'time_filter': 'day',
        'limit': 100,
        },
        dag=dag,
    )
