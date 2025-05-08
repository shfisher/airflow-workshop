from datetime import datetime, timedelta
import requests
import logging
import os
import pandas as pd
import io
import boto3

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

# Define a single DAG for the entire pipeline
nba_pipeline_dag = DAG(
    'nba_heights_pipeline',
    default_args=default_args,
    description='Download, process and upload NBA heights data to S3',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['nba', 'data', 's3', 'pipeline'],
    catchup=False,
)

def download_and_upload_to_s3(**context):
    """
    Downloads NBA heights data from OpenIntro website and uploads directly to S3.
    """
    # URL details
    url = 'https://www.openintro.org/data/csv/nba_heights.csv'
    
    # S3 details
    bucket_name = 'shemtov-testing-080525-01'
    s3_key = f'nba_heights/nba_heights_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    region_name = 'ap-southeast-1'  # Explicitly set the region
    
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
        logging.info(f"Uploading data to S3 bucket {bucket_name} with key {s3_key} in region {region_name}")
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_client = boto3.client(
            's3',
            region_name=region_name,
            aws_access_key_id=s3_hook.get_credentials().access_key,
            aws_secret_access_key=s3_hook.get_credentials().secret_key
        )
        
        # Upload using the client directly
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=response.content,
            ContentType='text/csv'
        )
        
        s3_path = f's3://{bucket_name}/{s3_key}'
        logging.info(f"Successfully uploaded NBA heights data to S3: {s3_path}")
        
        # Store the S3 path for the next task
        context['ti'].xcom_push(key='raw_s3_file_path', value=s3_path)
        return s3_path
        
    except Exception as e:
        logging.error(f"Failed to download and upload NBA heights data: {str(e)}")
        raise  # Re-raise the exception to fail the task

def clean_nba_data(**context):
    """
    Retrieves NBA heights data from S3, cleans it, and uploads the processed data back to S3.
    
    Cleaning steps:
    1. Create a full player name column by combining first and last names
    2. Display heights in metric units (meters and centimeters)
    3. Categorize players by position based on their height
    4. Sort players by height and reset index
    5. Log statistics about tallest and shortest players
    """
    # Get the S3 path from the previous task
    raw_s3_path = context['ti'].xcom_pull(task_ids='download_and_upload_to_s3', key='raw_s3_file_path')
    
    if not raw_s3_path:
        raise Exception("Raw S3 file path not found in XCom")
    
    # Parse bucket and key from the S3 path
    s3_path_parts = raw_s3_path.replace('s3://', '').split('/', 1)
    bucket_name = s3_path_parts[0]
    source_key = s3_path_parts[1]
    region_name = 'us-east-1'
    
    # Get S3 client
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_client = boto3.client(
        's3',
        region_name=region_name,
        aws_access_key_id=s3_hook.get_credentials().access_key,
        aws_secret_access_key=s3_hook.get_credentials().secret_key
    )
    
    try:
        logging.info(f"Processing NBA heights data from: s3://{bucket_name}/{source_key}")
        
        # Download the file from S3
        obj = s3_client.get_object(Bucket=bucket_name, Key=source_key)
        data = obj['Body'].read()
        
        # Load data into pandas DataFrame
        df = pd.read_csv(io.BytesIO(data))
        
        # Log the column names to help debug
        logging.info(f"DataFrame columns: {df.columns.tolist()}")
        
        # 1. Create a full player name column using the correct column names
        df['full_name'] = df['first_name'] + ' ' + df['last_name']
        
        # 2. Use the correct height columns
        # If h_meters already exists, use it directly
        if 'h_meters' in df.columns:
            df['height_meters'] = df['h_meters']
        # Otherwise, convert from inches if h_in exists
        elif 'h_in' in df.columns:
            df['height_meters'] = df['h_in'].apply(lambda x: round(x * 0.0254, 2))
        else:
            raise Exception("Neither h_meters nor h_in column found in the data")
        
        # Add height in cm if needed
        if 'h_in' in df.columns:
            df['height_cm'] = df['h_in'].apply(lambda x: round(x * 2.54, 1))
        else:
            # If we only have meters, convert to cm
            df['height_cm'] = df['height_meters'].apply(lambda x: round(x * 100, 1))
        
        # 3. Categorize players by position based on height
        def categorize_position(height_m):
            if height_m < 1.85:
                return 'Point Guard'
            elif height_m < 1.95:
                return 'Shooting Guard'
            elif height_m < 2.05:
                return 'Small Forward'
            elif height_m < 2.15:
                return 'Power Forward'
            else:
                return 'Center'
        
        df['position_category'] = df['height_meters'].apply(categorize_position)
        
        # 4. Sort players by height and reset index
        df = df.sort_values(by='height_meters', ascending=False).reset_index(drop=True)
        
        # 5. Log statistics about tallest and shortest players
        tallest_player = df.iloc[0]
        shortest_player = df.iloc[-1]
        
        logging.info(f"Tallest player: {tallest_player['full_name']} - {tallest_player['height_meters']}m ({tallest_player['height_cm']}cm)")
        logging.info(f"Shortest player: {shortest_player['full_name']} - {shortest_player['height_meters']}m ({shortest_player['height_cm']}cm)")
        
        # Log position category distribution
        position_counts = df['position_category'].value_counts()
        logging.info(f"Position category distribution:\n{position_counts}")
        
        # Calculate and log average height by position
        avg_height_by_position = df.groupby('position_category')['height_meters'].mean().round(2)
        logging.info(f"Average height by position category:\n{avg_height_by_position}")
        
        # Prepare the processed data for upload
        processed_key = f'processed_nba_heights/nba_heights_processed_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        
        # Convert DataFrame to CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Upload processed data to S3
        logging.info(f"Uploading processed data to S3: s3://{bucket_name}/{processed_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=processed_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        processed_s3_path = f's3://{bucket_name}/{processed_key}'
        logging.info(f"Successfully uploaded processed NBA heights data to S3: {processed_s3_path}")
        
        # Store the processed S3 path for potential downstream tasks
        context['ti'].xcom_push(key='processed_s3_path', value=processed_s3_path)
        return processed_s3_path
        
    except Exception as e:
        logging.error(f"Failed to process NBA heights data: {str(e)}")
        raise  # Re-raise the exception to fail the task

# Define the tasks in the single DAG
download_task = PythonOperator(
    task_id='download_and_upload_to_s3',
    python_callable=download_and_upload_to_s3,
    provide_context=True,
    dag=nba_pipeline_dag,
)

clean_task = PythonOperator(
    task_id='clean_nba_data',
    python_callable=clean_nba_data,
    provide_context=True,
    dag=nba_pipeline_dag,
)

# Set task dependencies within the single DAG
download_task >> clean_task
