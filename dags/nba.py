from datetime import datetime, timedelta
import requests
import logging
import os
import tempfile
import time

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

def download_nba_data(**context):
    """
    Downloads NBA heights data from OpenIntro website and saves to a temporary file.
    """
    # URL details
    url = 'https://www.openintro.org/data/csv/nba_heights.csv'
    
    # Set up headers for the request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/csv,application/csv,text/plain,application/octet-stream,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.openintro.org/'
    }
    
    # Create a temporary file to store the downloaded data
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
        temp_file_path = temp_file.name
    
    try:
        # Download the data with minimal retry
        logging.info(f"Attempting to download NBA heights data from {url}")
        response = requests.get(url, headers=headers, timeout=10)  # Shorter timeout
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Write the data to the temporary file
        with open(temp_file_path, 'wb') as f:
            f.write(response.content)
        
        logging.info(f"Successfully downloaded NBA heights data to {temp_file_path}")
        
        # Pass the file path to the next task
        context['ti'].xcom_push(key='temp_file_path', value=temp_file_path)
        return temp_file_path
        
    except Exception as e:
        # Clean up the temporary file if download fails
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        logging.error(f"Failed to download NBA heights data: {str(e)}")
        raise  # Re-raise the exception to fail the task

def upload_to_s3(**context):
    """
    Uploads the downloaded file to S3 bucket.
    """
    # Get the temporary file path from the previous task
    temp_file_path = context['ti'].xcom_pull(task_ids='download_nba_data', key='temp_file_path')
    
    if not temp_file_path or not os.path.exists(temp_file_path):
        raise Exception("Temporary file not found or does not exist")
    
    try:
        # S3 bucket details
        bucket_name = 'shemtov-testing-080525'
        s3_key = f'nba_heights/nba_heights_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        
        logging.info(f"Uploading data to S3 bucket {bucket_name} with key {s3_key}")
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_file(
            filename=temp_file_path,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        logging.info(f"Successfully uploaded NBA heights data to S3: s3://{bucket_name}/{s3_key}")
        
        # Add the S3 path to XCom for potential downstream tasks
        context['ti'].xcom_push(key='s3_file_path', value=f's3://{bucket_name}/{s3_key}')
        return f's3://{bucket_name}/{s3_key}'
        
    except Exception as e:
        logging.error(f"Failed to upload file to S3: {str(e)}")
        raise  # Re-raise the exception to fail the task
    
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            logging.info(f"Removed temporary file: {temp_file_path}")

# Define the tasks
download_task = PythonOperator(
    task_id='download_nba_data',
    python_callable=download_nba_data,
    provide_context=True,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
download_task >> upload_task
