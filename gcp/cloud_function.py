import os
from google.cloud import bigquery
from google.cloud import storage
import base64
import json

def gcs_to_bq(event, context):
    """Triggered by gcs notification to load all files from GCS to BigQuery."""
    
    # Define your GCS bucket and BigQuery dataset/table
    bucket_name = 'bucket_name'
    dataset_id = 'dataset_id'
    table_id = 'table_id'
    # Initialize clients
    storage_client = storage.Client()
    bq_client = bigquery.Client()
   
    # Prepare load job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,  
        autodetect=True,  
    )
   
    # Decode the Pub/Sub message
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    
    # Parse the message as JSON
    message_data = json.loads(pubsub_message)
    
    # Extract the bucket name and file name
    bucket_name = message_data['bucket']
    file_name = message_data['name']
    
    
    # Load each file into BigQuery
    if 'parquet' in file_name:
        uri = f"gs://{bucket_name}/{file_name}"
        
        # Load data from GCS to BigQuery
        load_job = bq_client.load_table_from_uri(
            uri,
            f"{dataset_id}.{table_id}",
            job_config=job_config,
        )
        
        # Wait for the job to complete
        load_job.result()
        
        print(f"Loaded {file_name} into {dataset_id}.{table_id}.")
    
    return 'All files loaded successfully!'
