from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta 
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.reddit_pipeline import (extract_reddit_posts_data, load_raw_posts_to_mongo, 
                                       extract_reddit_comments_data, load_raw_comments_to_mongo, 
                                       run_mongo_aggregation, run_transform_pipeline)
default_args = {
    'owner': 'zain',
    'start_date': datetime(2025, 10, 17),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),}

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
extract_posts_task = PythonOperator(
    task_id='extract_reddit_data',
    python_callable=extract_reddit_posts_data,
    op_kwargs={
        'subreddits': ['Egypt', 'CAIRO', 'AlexandriaEgy', 'Masr'],
        'time_filter': 'day',
        'limit': 100,
    },
    dag=dag,
)

extract_comments_task = PythonOperator(
    task_id='extract_comments_task',
    python_callable=extract_reddit_comments_data,
    op_kwargs={
        'subreddits': ['Egypt', 'CAIRO', 'AlexandriaEgy', 'Masr'],
        'time_filter': 'day',
        'limit': 10,
    },
    dag=dag,
)

mongo_posts_task = PythonOperator(
    task_id='load_data_to_mongo',
    python_callable=load_raw_posts_to_mongo,
    provide_context=True,
    dag=dag,
)

mongo_comments_task = PythonOperator(
    task_id='load_comments_to_mongo',
    python_callable=load_raw_comments_to_mongo,
    provide_context=True,
    dag=dag,
)
aggregate_task = PythonOperator(
    task_id='merge_reddit_data',
    python_callable=run_mongo_aggregation,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_and_clean_data',
    python_callable=run_transform_pipeline,
    provide_context=True,
    dag=dag,
)

extract_posts_task >> mongo_posts_task
extract_comments_task >> mongo_comments_task
[mongo_posts_task, mongo_comments_task] >> aggregate_task >> transform_task
