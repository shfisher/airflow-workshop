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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
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

def download_nba_data_and_upload_to_s3(**context):
    """
    Downloads NBA heights data from OpenIntro website and uploads to S3 bucket.
    Includes error handling and retry logic.
    """
    # URL and S3 bucket details
    url = 'https://www.openintro.org/data/csv/nba_heights.csv'
    bucket_name = 'shemtov-testing-080525'
    s3_key = f'nba_heights/nba_heights_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
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
        # Download the data with retry logic
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                logging.info(f"Attempting to download NBA heights data from {url}")
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()  # Raise an exception for HTTP errors
                
                # Write the data to the temporary file
                with open(temp_file_path, 'wb') as f:
                    f.write(response.content)
                
                logging.info(f"Successfully downloaded NBA heights data to {temp_file_path}")
                break  # Exit the retry loop if successful
                
            except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
                retry_count += 1
                if retry_count >= max_retries:
                    raise Exception(f"Failed to download NBA heights data after {max_retries} attempts: {str(e)}")
                logging.warning(f"Download attempt {retry_count} failed: {str(e)}. Retrying...")
                # Wait before retrying (exponential backoff)
                time.sleep(2 ** retry_count)
        
        # Upload the file to S3
        try:
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
            
        except Exception as e:
            raise Exception(f"Failed to upload file to S3: {str(e)}")
            
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            logging.info(f"Removed temporary file: {temp_file_path}")

# Define the task
download_and_upload_task = PythonOperator(
    task_id='download_nba_data_and_upload_to_s3',
    python_callable=download_nba_data_and_upload_to_s3,
    provide_context=True,
    dag=dag,
)

# Set task dependencies (if we add more tasks in the future)
download_and_upload_task
