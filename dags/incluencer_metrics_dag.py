from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from source.extractors.google_sheet_extractor import extract_sheet_data
from source.extractors.youtube_scraper import scrape_youtube_data
from source.loaders.snowflake_loader import load_to_snowflake

default_args = {
    'owner': 'Eyal Ozeri',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'influencer_campaign_metrics',
    default_args=default_args,
    catchup=False,
    description='Extract influencer campaign data, enrich with YouTube data, and load to Snowflake',
    schedule_interval=timedelta(days=1),
)

# Task 1: Extract data from Google Sheet
extract_sheet_task = PythonOperator(
    task_id='extract_google_sheet',
    python_callable=extract_sheet_data,
    dag=dag,
)

# Task 2: Scrape YouTube data
scrape_youtube_task = PythonOperator(
    task_id='scrape_youtube_data',
    python_callable=scrape_youtube_data,
    op_kwargs={'input_path': "{{ task_instance.xcom_pull(task_ids='extract_google_sheet') }}"},
    dag=dag,
)

# Task 3: Load to Snowflake
load_snowflake_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    op_kwargs={'input_path': "{{ task_instance.xcom_pull(task_ids='scrape_youtube_data') }}"},
    dag=dag,
)

# Dependencies
extract_sheet_task >> scrape_youtube_task >> load_snowflake_task