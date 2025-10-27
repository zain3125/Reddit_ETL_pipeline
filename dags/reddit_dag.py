from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime 
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.reddit_pipeline import extract_reddit_data, load_data_to_database, load_data_to_csv_task
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
        'file_name': f'reddit_posts_{datetime.now().strftime("%Y%m%d")}',
        'subreddits': ['Egypt', 'CAIRO', 'AlexandriaEgy', 'Masr'],
        'time_filter': 'day',
        'limit': 100,
    },
    dag=dag,
)

load_to_csv_task = PythonOperator(
    task_id='load_to_csv',
    python_callable=load_data_to_csv_task,
    provide_context=True,
    op_kwargs={'file_path': f"/opt/airflow/data/output/reddit_posts_{datetime.now().strftime('%Y%m%d')}.csv"},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_database',
    python_callable=load_data_to_database,
    provide_context=True,
    dag=dag,
)

extract_task >> [load_to_csv_task, load_task]
