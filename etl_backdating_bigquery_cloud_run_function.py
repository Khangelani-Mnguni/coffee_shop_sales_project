# main.py

import os
import pandas as pd
import functions_framework
import logging
import google.cloud.bigquery
from google.cloud import storage

# Set up logging for visibility and easy debugging.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# BigQuery project and dataset configuration
# The project ID is now hardcoded to prevent errors from missing environment variables.
PROJECT_ID = "glassy-augury-463218-e4"
DATASET_ID = "coffee_sales_data"

# =========================================================
# 1️⃣ BigQuery Table Schemas
# =========================================================
# Define the schemas for the BigQuery tables.

dim_date_schema = [
    google.cloud.bigquery.SchemaField("date_id", "INTEGER", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("transaction_date", "DATE", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("day", "INTEGER"),
    google.cloud.bigquery.SchemaField("month", "INTEGER"),
    google.cloud.bigquery.SchemaField("year", "INTEGER"),
    google.cloud.bigquery.SchemaField("weekday", "STRING"),
]

dim_time_schema = [
    google.cloud.bigquery.SchemaField("time_id", "INTEGER", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("transaction_time", "TIME", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("hour", "INTEGER"),
    google.cloud.bigquery.SchemaField("minute", "INTEGER"),
    google.cloud.bigquery.SchemaField("second", "INTEGER"),
]

dim_store_schema = [
    google.cloud.bigquery.SchemaField("store_id", "INTEGER", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("store_location", "STRING", mode="REQUIRED"),
]

dim_product_schema = [
    google.cloud.bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("unit_price", "FLOAT", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("product_category", "STRING"),
    google.cloud.bigquery.SchemaField("product_type", "STRING"),
    google.cloud.bigquery.SchemaField("product_detail", "STRING"),
]

fact_sales_schema = [
    google.cloud.bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("date_id", "INTEGER", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("time_id", "INTEGER", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("store_id", "INTEGER", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("transaction_qty", "FLOAT", mode="REQUIRED"),
    google.cloud.bigquery.SchemaField("total_amount", "FLOAT"),
]

# A dictionary to map table names to their schemas
table_schemas = {
    "dim_date": dim_date_schema,
    "dim_time": dim_time_schema,
    "dim_store": dim_store_schema,
    "dim_product": dim_product_schema,
    "fact_sales": fact_sales_schema,
}

def create_table_if_not_exists(client, table_id, schema):
    """Creates a BigQuery table if it does not already exist."""
    try:
        table_ref = client.dataset(DATASET_ID).table(table_id)
        client.get_table(table_ref)  # API call
        logging.info(f"Table {table_id} already exists.")
    except google.api_core.exceptions.NotFound:
        logging.info(f"Table {table_id} not found. Creating table...")
        table = google.cloud.bigquery.Table(table_ref, schema=schema)
        client.create_table(table)  # API call
        logging.info(f"Table {table_id} created successfully.")

# =========================================================
# 2️⃣ ETL Functions
# =========================================================

def transform_data(df_raw):
    """
    Transforms raw CSV data into a star schema and returns DataFrames
    for each dimension and fact table.
    """
    logging.info("🔄 Transforming data for BigQuery...")

    # Calculate total_amount
    df_raw["total_amount"] = df_raw["transaction_qty"] * df_raw["unit_price"]
    
    # Generate keys for joining
    df_raw['date_key'] = pd.to_datetime(df_raw['transaction_date']).dt.date.astype(str)
    df_raw['time_key'] = pd.to_datetime(df_raw['transaction_time'], format='%H:%M:%S').dt.time.astype(str)
    
    # Create unique keys for dimensions and map them back to the original dataframe.
    
    # Date Dimension
    dim_date = df_raw[['date_key']].drop_duplicates().sort_values('date_key').reset_index(drop=True)
    dim_date['date_id'] = dim_date.index
    dim_date['transaction_date'] = pd.to_datetime(dim_date['date_key'])
    dim_date['day'] = dim_date['transaction_date'].dt.day
    dim_date['month'] = dim_date['transaction_date'].dt.month
    dim_date['year'] = dim_date['transaction_date'].dt.year
    dim_date['weekday'] = dim_date['transaction_date'].dt.day_name()
    
    # Time Dimension
    dim_time = df_raw[['time_key']].drop_duplicates().sort_values('time_key').reset_index(drop=True)
    dim_time['time_id'] = dim_time.index
    dim_time['transaction_time'] = pd.to_datetime(dim_time['time_key']).dt.time
    dim_time['hour'] = dim_time['transaction_time'].apply(lambda x: x.hour)
    dim_time['minute'] = dim_time['transaction_time'].apply(lambda x: x.minute)
    dim_time['second'] = dim_time['transaction_time'].apply(lambda x: x.second)

    # Store Dimension
    dim_store = df_raw[['store_id', 'store_location']].drop_duplicates().sort_values('store_id').reset_index(drop=True)

    # Product Dimension
    dim_product_cols = ['product_id', 'unit_price', 'product_category', 'product_type', 'product_detail']
    dim_product = df_raw[dim_product_cols].drop_duplicates().sort_values('product_id').reset_index(drop=True)

    # Fact Table - use the generated keys to merge and get the correct IDs
    df_fact = df_raw.merge(dim_date[['date_key', 'date_id']], on='date_key')
    df_fact = df_fact.merge(dim_time[['time_key', 'time_id']], on='time_key')
    
    # Finalize the fact table with correct columns
    df_fact = df_fact[[
        "transaction_id",
        "date_id",
        "time_id",
        "store_id",
        "product_id",
        "transaction_qty",
        "total_amount"
    ]]

    logging.info(f"✅ Transformation completed. Records: {len(df_fact)}")
    return dim_date, dim_time, dim_store, dim_product, df_fact

def load_data_to_bigquery(client, dim_date, dim_time, dim_store, dim_product, df_fact):
    """
    Loads DataFrames into their respective BigQuery tables, overwriting existing data.
    """
    logging.info("📤 Loading data to BigQuery...")

    # Load dimension tables first
    dim_tables = {
        "dim_date": dim_date[['date_id', 'transaction_date', 'day', 'month', 'year', 'weekday']],
        "dim_time": dim_time[['time_id', 'transaction_time', 'hour', 'minute', 'second']],
        "dim_store": dim_store,
        "dim_product": dim_product,
    }

    for table_name, df in dim_tables.items():
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        job_config = google.cloud.bigquery.LoadJobConfig(
            write_disposition=google.cloud.bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=False,
            schema=table_schemas[table_name],
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete.
        logging.info(f"Loaded {job.output_rows} rows into {table_id}.")

    # Load fact table
    fact_table_id = f"{PROJECT_ID}.{DATASET_ID}.fact_sales"
    job_config = google.cloud.bigquery.LoadJobConfig(
            write_disposition=google.cloud.bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=False,
            schema=fact_sales_schema,
    )
    job = client.load_table_from_dataframe(df_fact, fact_table_id, job_config=job_config)
    job.result()
    logging.info(f"Loaded {job.output_rows} rows into {fact_table_id}.")

    logging.info("✅ Data loaded to BigQuery successfully!")


@functions_framework.cloud_event
def process_gcs_file(cloud_event):
    """
    Background Cloud Function to be triggered by a new file upload to GCS.
    """
    try:
        # Get event data
        data = cloud_event.data
        bucket_name = data["bucket"]
        file_name = data["name"]

        logging.info(f"📥 Processing new file: gs://{bucket_name}/{file_name}")

        # Check if the file is the one we want to process
        if file_name != "transformed.csv":
            logging.info(f"File {file_name} is not the expected file. Skipping.")
            return

        # Initialize clients
        storage_client = storage.Client()
        bigquery_client = google.cloud.bigquery.Client()

        # Download the CSV file from GCS
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        # Read the CSV directly into a pandas DataFrame
        df_raw = pd.read_csv(blob.open("rb"))
        logging.info(f"✅ Successfully read {len(df_raw)} records from {file_name}.")

        # --- Transform ---
        dim_date, dim_time, dim_store, dim_product, df_fact = transform_data(df_raw)

        # --- Load ---
        # Ensure BigQuery dataset exists
        try:
            bigquery_client.get_dataset(bigquery_client.dataset(DATASET_ID))
            logging.info(f"Dataset {DATASET_ID} already exists.")
        except google.api_core.exceptions.NotFound:
            logging.info(f"Dataset {DATASET_ID} not found. Creating...")
            dataset = bigquery_client.create_dataset(DATASET_ID)
            logging.info(f"Dataset {dataset.dataset_id} created.")

        # Create tables if they don't exist
        for table_name, schema in table_schemas.items():
            create_table_if_not_exists(bigquery_client, table_name, schema)

        # Load the data into BigQuery
        load_data_to_bigquery(bigquery_client, dim_date, dim_time, dim_store, dim_product, df_fact)
        
        logging.info("✅ ETL process completed successfully.")

    except Exception as e:
        logging.error(f"❌ An error occurred during the ETL process: {e}")
        # You could also add more robust error handling, like sending a notification.
