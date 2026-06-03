from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import glob
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

PROJECT_ID = "coffee-shop-updated"
DATASET = "coffee_raw"
DATA_PATH = "/opt/airflow/project/data"

default_args = {
    "owner": "Khangelani",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

# ---------------------------
# SCHEMA (WITH ingestion_timestamp)
# ---------------------------

SCHEMA = [
    bigquery.SchemaField("transaction_id", "INTEGER"),
    bigquery.SchemaField("transaction_date", "STRING"),
    bigquery.SchemaField("transaction_time", "STRING"),
    bigquery.SchemaField("transaction_qty", "INTEGER"),
    bigquery.SchemaField("store_id", "INTEGER"),
    bigquery.SchemaField("store_location", "STRING"),
    bigquery.SchemaField("product_id", "INTEGER"),
    bigquery.SchemaField("unit_price", "FLOAT"),
    bigquery.SchemaField("product_category", "STRING"),
    bigquery.SchemaField("product_type", "STRING"),
    bigquery.SchemaField("product_detail", "STRING"),
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
]

# ---------------------------
# Helper Functions
# ---------------------------

def list_csv_files(**context):
    files = glob.glob(f"{DATA_PATH}/*.csv")
    if not files:
        raise ValueError("No CSV files found in data folder")
    context["ti"].xcom_push(key="files", value=files)


def process_file(**context):
    client = bigquery.Client()
    files = context["ti"].xcom_pull(key="files")

    for file in files:
        store_name = os.path.basename(file).replace(".csv", "").lower().replace(" ", "_")
        table_id = f"{PROJECT_ID}.{DATASET}.raw_{store_name}"

        print(f"Processing file: {file} → {table_id}")

        # ---------------------------
        # LOAD INTO PANDAS
        # ---------------------------
        df = pd.read_csv(file)

        # ---------------------------
        # TYPE ENFORCEMENT
        # ---------------------------
        df["transaction_id"] = df["transaction_id"].astype("int64")
        df["transaction_qty"] = df["transaction_qty"].astype("int64")
        df["store_id"] = df["store_id"].astype("int64")
        df["product_id"] = df["product_id"].astype("int64")
        df["unit_price"] = df["unit_price"].astype("float64")

        # ---------------------------
        # ADD INGESTION TIMESTAMP
        # ---------------------------
        df["ingestion_timestamp"] = pd.Timestamp.utcnow()

        # ---------------------------
        # CHECK TABLE EXISTS
        # ---------------------------
        try:
            client.get_table(table_id)
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        except NotFound:
            write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

        # ---------------------------
        # LOAD CONFIG (NO AUTODETECT)
        # ---------------------------
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition=write_disposition,
        )

        # ---------------------------
        # LOAD DATA
        # ---------------------------
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        print(f"Loaded {len(df)} rows into {table_id}")


# ---------------------------
# DAG
# ---------------------------

with DAG(
    dag_id="coffee_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
) as dag:

    # 1. Detect files
    detect_files = PythonOperator(
        task_id="detect_files",
        python_callable=list_csv_files,
    )

    # 2. Ingest data
    ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=process_file,
    )

    # 3. dbt staging
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="""
        cd /opt/airflow/project/transform/coffee_project &&
        dbt run --select staging --target prod
        """
    )

    # 4. dbt marts
    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command="""
        cd /opt/airflow/project/transform/coffee_project &&
        dbt run --select marts --target prod
        """
    )

    # 5. dbt tests
    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command="""
        cd /opt/airflow/project/transform/coffee_project &&
        dbt test --target prod
        """
    )

    # FLOW
    detect_files >> ingest >> dbt_staging >> dbt_marts >> dbt_tests