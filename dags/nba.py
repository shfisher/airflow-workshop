from datetime import datetime, timedelta
import requests
import logging
import os
import pandas as pd
import io
import boto3
import json
import numpy as np
from collections import defaultdict

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

def analyze_nba_data(**context):
    """
    Retrieves processed NBA heights data from S3, performs analysis and visualization,
    and uploads a JSON report to S3.
    
    Analysis steps:
    1. Calculate statistics by position category
    2. Generate ASCII-based histograms
    3. Create text-based box plots
    4. Identify top 5 tallest and shortest players
    5. Generate a comprehensive JSON report
    """
    # Get the S3 path from the previous task
    processed_s3_path = context['ti'].xcom_pull(task_ids='clean_nba_data', key='processed_s3_path')
    
    if not processed_s3_path:
        raise Exception("Processed S3 file path not found in XCom")
    
    # Parse bucket and key from the S3 path
    s3_path_parts = processed_s3_path.replace('s3://', '').split('/', 1)
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
        logging.info(f"Analyzing NBA heights data from: s3://{bucket_name}/{source_key}")
        
        # Download the file from S3
        obj = s3_client.get_object(Bucket=bucket_name, Key=source_key)
        data = obj['Body'].read()
        
        # Load data into pandas DataFrame
        df = pd.read_csv(io.BytesIO(data))
        
        # Initialize the results dictionary for the JSON report
        results = {
            "analysis_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_source": processed_s3_path,
            "total_players": len(df),
            "position_statistics": {},
            "height_distribution": {},
            "box_plots": {},
            "top_players": {
                "tallest": [],
                "shortest": []
            }
        }
        
        # 1. Calculate statistics by position category
        position_stats = {}
        for position in df['position_category'].unique():
            position_df = df[df['position_category'] == position]
            stats = {
                "count": len(position_df),
                "height_meters_mean": round(position_df['height_meters'].mean(), 2),
                "height_meters_median": round(position_df['height_meters'].median(), 2),
                "height_meters_std": round(position_df['height_meters'].std(), 2),
                "height_meters_min": round(position_df['height_meters'].min(), 2),
                "height_meters_max": round(position_df['height_meters'].max(), 2),
                "height_cm_mean": round(position_df['height_cm'].mean(), 1),
                "height_cm_median": round(position_df['height_cm'].median(), 1),
                "height_cm_std": round(position_df['height_cm'].std(), 1),
                "height_cm_min": round(position_df['height_cm'].min(), 1),
                "height_cm_max": round(position_df['height_cm'].max(), 1),
            }
            position_stats[position] = stats
            
        results["position_statistics"] = position_stats
        
        # 2. Generate ASCII-based histograms for height distribution
        def generate_ascii_histogram(data, bins=10, max_bar_length=50):
            hist, bin_edges = np.histogram(data, bins=bins)
            max_count = max(hist)
            
            histogram_data = []
            for i, count in enumerate(hist):
                bar_length = int(count / max_count * max_bar_length) if max_count > 0 else 0
                bar = '#' * bar_length
                bin_range = f"{bin_edges[i]:.1f}-{bin_edges[i+1]:.1f}"
                histogram_data.append({
                    "bin_range": bin_range,
                    "count": int(count),
                    "visualization": bar
                })
                
            return histogram_data
        
        # Generate histogram for all players
        results["height_distribution"]["all_players_cm"] = generate_ascii_histogram(df['height_cm'], bins=15)
        
        # Generate histograms by position
        for position in df['position_category'].unique():
            position_df = df[df['position_category'] == position]
            results["height_distribution"][f"{position}_cm"] = generate_ascii_histogram(position_df['height_cm'], bins=10)
        
        # 3. Create text-based box plots
        def generate_text_boxplot(data, width=40):
            q1, median, q3 = np.percentile(data, [25, 50, 75])
            iqr = q3 - q1
            lower_whisker = max(min(data), q1 - 1.5 * iqr)
            upper_whisker = min(max(data), q3 + 1.5 * iqr)
            
            # Scale to width
            data_range = upper_whisker - lower_whisker
            scale = width / data_range if data_range > 0 else 1
            
            # Calculate positions
            lower_whisker_pos = 0
            q1_pos = int((q1 - lower_whisker) * scale)
            median_pos = int((median - lower_whisker) * scale)
            q3_pos = int((q3 - lower_whisker) * scale)
            upper_whisker_pos = int((upper_whisker - lower_whisker) * scale)
            
            # Create the box plot
            boxplot = ['-'] * (upper_whisker_pos + 1)
            boxplot[lower_whisker_pos] = '|'  # Lower whisker
            boxplot[upper_whisker_pos] = '|'  # Upper whisker
            
            # Draw the box
            for i in range(q1_pos, q3_pos + 1):
                boxplot[i] = '#'
                
            boxplot[median_pos] = 'M'  # Median
            
            return {
                "visualization": ''.join(boxplot),
                "statistics": {
                    "lower_whisker": round(lower_whisker, 1),
                    "q1": round(q1, 1),
                    "median": round(median, 1),
                    "q3": round(q3, 1),
                    "upper_whisker": round(upper_whisker, 1),
                    "iqr": round(iqr, 1)
                }
            }
        
        # Generate box plot for all players
        results["box_plots"]["all_players_cm"] = generate_text_boxplot(df['height_cm'])
        
        # Generate box plots by position
        for position in df['position_category'].unique():
            position_df = df[df['position_category'] == position]
            results["box_plots"][f"{position}_cm"] = generate_text_boxplot(position_df['height_cm'])
        
        # 4. Identify top 5 tallest and shortest players
        # Get the 5 tallest players
        tallest_players = df.sort_values(by='height_meters', ascending=False).head(5)
        for _, player in tallest_players.iterrows():
            results["top_players"]["tallest"].append({
                "name": player['full_name'],
                "height_meters": player['height_meters'],
                "height_cm": player['height_cm'],
                "position_category": player['position_category']
            })
            
        # Get the 5 shortest players
        shortest_players = df.sort_values(by='height_meters', ascending=True).head(5)
        for _, player in shortest_players.iterrows():
            results["top_players"]["shortest"].append({
                "name": player['full_name'],
                "height_meters": player['height_meters'],
                "height_cm": player['height_cm'],
                "position_category": player['position_category']
            })
        
        # 5. Generate a comprehensive JSON report
        # Prepare the JSON report for upload
        report_key = f'analysis_reports/nba_heights_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        
        # Convert the results to JSON
        json_data = json.dumps(results, indent=2)
        
        # Upload JSON report to S3
        logging.info(f"Uploading analysis report to S3: s3://{bucket_name}/{report_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=report_key,
            Body=json_data,
            ContentType='application/json'
        )
        
        report_s3_path = f's3://{bucket_name}/{report_key}'
        logging.info(f"Successfully uploaded NBA heights analysis report to S3: {report_s3_path}")
        
        # Store the report S3 path for potential downstream tasks
        context['ti'].xcom_push(key='report_s3_path', value=report_s3_path)
        return report_s3_path
        
    except Exception as e:
        logging.error(f"Failed to analyze NBA heights data: {str(e)}")
        raise  # Re-raise the exception to fail the task

def generate_html_dashboard(**context):
    """
    Creates an HTML dashboard from the NBA analysis results.
    
    Features:
    1. Create an HTML dashboard with ASCII/text-based visualizations
    2. Format the data in readable tables
    3. Apply modern styling with responsive design
    """
    # Get the S3 path from the previous task
    report_s3_path = context['ti'].xcom_pull(task_ids='analyze_nba_data', key='report_s3_path')
    
    if not report_s3_path:
        raise Exception("Analysis report S3 path not found in XCom")
    
    # Parse bucket and key from the S3 path
    s3_path_parts = report_s3_path.replace('s3://', '').split('/', 1)
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
        logging.info(f"Generating HTML dashboard from analysis report: s3://{bucket_name}/{source_key}")
        
        # Download the JSON report from S3
        obj = s3_client.get_object(Bucket=bucket_name, Key=source_key)
        data = obj['Body'].read().decode('utf-8')
        
        # Parse the JSON data
        analysis_results = json.loads(data)
        
        # Generate HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>NBA Heights Analysis Dashboard</title>
            <style>
                :root {{
                    --primary-color: #1d428a;
                    --secondary-color: #c8102e;
                    --background-color: #f8f9fa;
                    --text-color: #333;
                    --border-color: #ddd;
                    --box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                }}
                
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    line-height: 1.6;
                    color: var(--text-color);
                    background-color: var(--background-color);
                    margin: 0;
                    padding: 0;
                }}
                
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                
                header {{
                    background-color: var(--primary-color);
                    color: white;
                    padding: 20px;
                    text-align: center;
                    margin-bottom: 30px;
                    border-radius: 5px;
                    box-shadow: var(--box-shadow);
                }}
                
                h1, h2, h3, h4 {{
                    margin-top: 0;
                }}
                
                .dashboard-section {{
                    background-color: white;
                    border-radius: 5px;
                    box-shadow: var(--box-shadow);
                    margin-bottom: 30px;
                    padding: 20px;
                    overflow: auto;
                }}
                
                .dashboard-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 20px;
                }}
                
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-bottom: 20px;
                }}
                
                th, td {{
                    padding: 12px 15px;
                    text-align: left;
                    border-bottom: 1px solid var(--border-color);
                }}
                
                th {{
                    background-color: var(--primary-color);
                    color: white;
                }}
                
                tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                
                .visualization {{
                    font-family: monospace;
                    white-space: pre;
                    overflow-x: auto;
                    background-color: #f8f8f8;
                    padding: 15px;
                    border-radius: 5px;
                    border: 1px solid var(--border-color);
                    margin-bottom: 20px;
                }}
                
                .player-card {{
                    background-color: white;
                    border-radius: 5px;
                    box-shadow: var(--box-shadow);
                    padding: 15px;
                    margin-bottom: 15px;
                    border-left: 5px solid var(--primary-color);
                }}
                
                .tallest {{
                    border-left-color: var(--secondary-color);
                }}
                
                .stats-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 15px;
                }}
                
                .stat-card {{
                    background-color: white;
                    border-radius: 5px;
                    box-shadow: var(--box-shadow);
                    padding: 15px;
                    text-align: center;
                }}
                
                .stat-value {{
                    font-size: 24px;
                    font-weight: bold;
                    color: var(--primary-color);
                }}
                
                .stat-label {{
                    font-size: 14px;
                    color: #666;
                }}
                
                footer {{
                    text-align: center;
                    padding: 20px;
                    margin-top: 30px;
                    color: #666;
                    font-size: 14px;
                }}
                
                @media (max-width: 768px) {{
                    .dashboard-grid {{
                        grid-template-columns: 1fr;
                    }}
                    
                    .stats-grid {{
                        grid-template-columns: 1fr 1fr;
                    }}
                }}
            </style>
        </head>
        <body>
            <header>
                <h1>NBA Heights Analysis Dashboard</h1>
                <p>Analysis generated on {analysis_results['analysis_timestamp']}</p>
            </header>
            
            <div class="container">
                <div class="dashboard-section">
                    <h2>Overview</h2>
                    <div class="stats-grid">
                        <div class="stat-card">
                            <div class="stat-value">{analysis_results['total_players']}</div>
                            <div class="stat-label">Total Players</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{len(analysis_results['position_statistics'])}</div>
                            <div class="stat-label">Position Categories</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{analysis_results['box_plots']['all_players_cm']['statistics']['median']} cm</div>
                            <div class="stat-label">Median Height</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{analysis_results['box_plots']['all_players_cm']['statistics']['iqr']} cm</div>
                            <div class="stat-label">Height IQR</div>
                        </div>
                    </div>
                </div>
                
                <div class="dashboard-section">
                    <h2>Position Statistics</h2>
                    <table>
                        <thead>
                            <tr>
                                <th>Position</th>
                                <th>Count</th>
                                <th>Avg Height (m)</th>
                                <th>Median Height (m)</th>
                                <th>Min Height (m)</th>
                                <th>Max Height (m)</th>
                            </tr>
                        </thead>
                        <tbody>
        """
        
        # Add position statistics to the table
        for position, stats in analysis_results['position_statistics'].items():
            html_content += f"""
                            <tr>
                                <td>{position}</td>
                                <td>{stats['count']}</td>
                                <td>{stats['height_meters_mean']}</td>
                                <td>{stats['height_meters_median']}</td>
                                <td>{stats['height_meters_min']}</td>
                                <td>{stats['height_meters_max']}</td>
                            </tr>
            """
        
        html_content += """
                        </tbody>
                    </table>
                </div>
                
                <div class="dashboard-section">
                    <h2>Height Distribution</h2>
                    <h3>All Players</h3>
                    <div class="visualization">
        """
        
        # Add histogram for all players
        for bin_data in analysis_results['height_distribution']['all_players_cm']:
            html_content += f"{bin_data['bin_range']} ({bin_data['count']}): {bin_data['visualization']}\n"
        
        html_content += """
                    </div>
                    
                    <h3>By Position</h3>
                    <div class="dashboard-grid">
        """
        
        # Add histograms by position
        for position in analysis_results['position_statistics'].keys():
            position_key = f"{position}_cm"
            if position_key in analysis_results['height_distribution']:
                html_content += f"""
                        <div>
                            <h4>{position}</h4>
                            <div class="visualization">
                """
                
                for bin_data in analysis_results['height_distribution'][position_key]:
                    html_content += f"{bin_data['bin_range']} ({bin_data['count']}): {bin_data['visualization']}\n"
                
                html_content += """
                            </div>
                        </div>
                """
        
        html_content += """
                    </div>
                </div>
                
                <div class="dashboard-section">
                    <h2>Height Distribution Box Plots</h2>
                    <h3>All Players</h3>
                    <div class="visualization">
        """
        
        # Add box plot for all players
        all_players_boxplot = analysis_results['box_plots']['all_players_cm']
        html_content += f"""
        {all_players_boxplot['statistics']['lower_whisker']} cm {all_players_boxplot['visualization']} {all_players_boxplot['statistics']['upper_whisker']} cm
        
        Lower Whisker: {all_players_boxplot['statistics']['lower_whisker']} cm
        Q1: {all_players_boxplot['statistics']['q1']} cm
        Median (M): {all_players_boxplot['statistics']['median']} cm
        Q3: {all_players_boxplot['statistics']['q3']} cm
        Upper Whisker: {all_players_boxplot['statistics']['upper_whisker']} cm
        """
        
        html_content += """
                    </div>
                    
                    <h3>By Position</h3>
                    <div class="dashboard-grid">
        """
        
        # Add box plots by position
        for position in analysis_results['position_statistics'].keys():
            position_key = f"{position}_cm"
            if position_key in analysis_results['box_plots']:
                boxplot = analysis_results['box_plots'][position_key]
                html_content += f"""
                        <div>
                            <h4>{position}</h4>
                            <div class="visualization">
        {boxplot['statistics']['lower_whisker']} cm {boxplot['visualization']} {boxplot['statistics']['upper_whisker']} cm
        
        Lower: {boxplot['statistics']['lower_whisker']} cm
        Q1: {boxplot['statistics']['q1']} cm
        Median: {boxplot['statistics']['median']} cm
        Q3: {boxplot['statistics']['q3']} cm
        Upper: {boxplot['statistics']['upper_whisker']} cm
                            </div>
                        </div>
                """
        
        html_content += """
                    </div>
                </div>
                
                <div class="dashboard-section">
                    <h2>Notable Players</h2>
                    <div class="dashboard-grid">
                        <div>
                            <h3>Tallest Players</h3>
        """
        
        # Add tallest players
        for player in analysis_results['top_players']['tallest']:
            html_content += f"""
                            <div class="player-card tallest">
                                <h4>{player['name']}</h4>
                                <p><strong>Height:</strong> {player['height_meters']} m ({player['height_cm']} cm)</p>
                                <p><strong>Position:</strong> {player['position_category']}</p>
                            </div>
            """
        
        html_content += """
                        </div>
                        <div>
                            <h3>Shortest Players</h3>
        """
        
        # Add shortest players
        for player in analysis_results['top_players']['shortest']:
            html_content += f"""
                            <div class="player-card">
                                <h4>{player['name']}</h4>
                                <p><strong>Height:</strong> {player['height_meters']} m ({player['height_cm']} cm)</p>
                                <p><strong>Position:</strong> {player['position_category']}</p>
                            </div>
            """
        
        html_content += """
                        </div>
                    </div>
                </div>
                
                <footer>
                    <p>NBA Heights Analysis Dashboard | Generated by Airflow Data Pipeline</p>
                    <p>Data Source: {analysis_results['data_source']}</p>
                </footer>
            </div>
        </body>
        </html>
        """
        
        # Prepare the HTML dashboard for upload
        dashboard_key = f'dashboards/nba_heights_dashboard_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html'
        
        # Upload HTML dashboard to S3
        logging.info(f"Uploading HTML dashboard to S3: s3://{bucket_name}/{dashboard_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=dashboard_key,
            Body=html_content,
            ContentType='text/html'
        )
        
        dashboard_s3_path = f's3://{bucket_name}/{dashboard_key}'
        logging.info(f"Successfully uploaded NBA heights dashboard to S3: {dashboard_s3_path}")
        
        # Store the dashboard S3 path for potential downstream tasks
        context['ti'].xcom_push(key='dashboard_s3_path', value=dashboard_s3_path)
        return dashboard_s3_path
        
    except Exception as e:
        logging.error(f"Failed to generate HTML dashboard: {str(e)}")
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

analyze_task = PythonOperator(
    task_id='analyze_nba_data',
    python_callable=analyze_nba_data,
    provide_context=True,
    dag=nba_pipeline_dag,
)

dashboard_task = PythonOperator(
    task_id='generate_html_dashboard',
    python_callable=generate_html_dashboard,
    provide_context=True,
    dag=nba_pipeline_dag,
)

# Set task dependencies within the single DAG
download_task >> clean_task >> analyze_task >> dashboard_task
