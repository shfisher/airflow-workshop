from datetime import datetime, timedelta
import requests
import logging
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,  # Reduced retries for faster failure
    'retry_delay': timedelta(minutes=1),  # Shorter retry delay
    'execution_timeout': timedelta(minutes=5),  # Shorter timeout
}

# Define the DAG
dag = DAG(
    'nba_heights_data_pipeline',
    default_args=default_args,
    description='Download NBA heights data and upload to S3',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['nba', 'data', 's3'],
    catchup=False,
)

def download_and_upload_to_s3(**context):
    """
    Downloads NBA heights data from OpenIntro website and uploads directly to S3.
    """
    # URL details
    url = 'https://www.openintro.org/data/csv/nba_heights.csv'
    
    # S3 details
    bucket_name = 'shemtov-testing-080525'
    s3_key = f'nba_heights/nba_heights_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    # Set up headers for the request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/csv,application/csv,text/plain,application/octet-stream,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.openintro.org/'
    }
    
    try:
        # Download the data
        logging.info(f"Attempting to download NBA heights data from {url}")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Upload directly to S3
        logging.info(f"Uploading data to S3 bucket {bucket_name} with key {s3_key}")
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(
            string_data=response.text,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        
        s3_path = f's3://{bucket_name}/{s3_key}'
        logging.info(f"Successfully uploaded NBA heights data to S3: {s3_path}")
        
        # Store the S3 path for potential downstream tasks
        context['ti'].xcom_push(key='s3_file_path', value=s3_path)
        return s3_path
        
    except Exception as e:
        logging.error(f"Failed to download and upload NBA heights data: {str(e)}")
        raise  # Re-raise the exception to fail the task

# Define the task
download_and_upload_task = PythonOperator(
    task_id='download_and_upload_to_s3',
    python_callable=download_and_upload_to_s3,
    provide_context=True,
    dag=dag,
)

# No dependencies needed for a single task
download_and_upload_task
