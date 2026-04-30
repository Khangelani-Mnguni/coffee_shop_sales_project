import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# --- ETL Logic ---
INPUT_FILE = os.path.join(os.path.dirname(__file__), "Coffee Shop Sales2.csv")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "transformed.csv")

def extract_data(file_path):
    try:
        # Try to auto-detect delimiter
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
        sep = ';' if ';' in first_line else ','

        df = pd.read_csv(file_path, sep=sep)
        df.columns = df.columns.str.strip()
        print(f"✅ Data extracted successfully from {file_path}")
        print("Columns in CSV:", df.columns.tolist())

        # Ensure 'transaction_date' exists
        if 'transaction_date' not in df.columns:
            print("❌ 'transaction_date' column not found in CSV.")
            return None

        return df
    except FileNotFoundError:
        print(f"❌ Error: The file {file_path} was not found.")
        return None
    except Exception as e:
        print(f"❌ An error occurred during data extraction: {e}")
        return None

def transform_data(df, start_date_str, end_date_str):
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    df_daily = df[
        (df['transaction_date'].dt.strftime('%Y-%m-%d') >= start_date_str) &
        (df['transaction_date'].dt.strftime('%Y-%m-%d') <= end_date_str)
    ]
    return df_daily

def load_data(df, file_path, start_date_str, end_date_str):
    mode = 'a' if os.path.exists(file_path) else 'w'
    header = not os.path.exists(file_path)
    df.to_csv(file_path, mode=mode, header=header, index=False)
    print(f"✅ Data for {start_date_str} loaded to {file_path}.")

def get_next_date_to_process(output_csv, df_raw):
    df_raw['transaction_date'] = pd.to_datetime(df_raw['transaction_date'], errors='coerce')
    if not os.path.exists(output_csv):
        return df_raw['transaction_date'].min()

    df_existing = pd.read_csv(output_csv)
    df_existing['transaction_date'] = pd.to_datetime(df_existing['transaction_date'], errors='coerce')
    df_existing = df_existing.dropna(subset=['transaction_date'])
    last_date = df_existing['transaction_date'].max()
    return last_date + timedelta(days=1)

def run_daily_incremental():
    df_raw = extract_data(INPUT_FILE)
    if df_raw is None:
        print("❌ Extraction failed.")
        return

    next_date = get_next_date_to_process(OUTPUT_FILE, df_raw)
    df_raw['transaction_date'] = pd.to_datetime(df_raw['transaction_date'], errors='coerce')
    df_raw = df_raw.dropna(subset=['transaction_date'])

    last_raw_date = df_raw['transaction_date'].max()
    if next_date > last_raw_date:
        print("✅ No new data to process. All dates up to latest available are loaded.")
        return

    start_date_str = next_date.strftime('%Y-%m-%d')
    end_date_str = next_date.strftime('%Y-%m-%d')

    print(f"📆 Processing daily backdate for: {start_date_str}")
    df_transformed = transform_data(df_raw, start_date_str, end_date_str)
    if df_transformed.empty:
        print(f"⚠ No data found for {start_date_str}")
        return

    load_data(df_transformed, OUTPUT_FILE, start_date_str, end_date_str)
    print(f"✅ Daily backdate for {start_date_str} completed.")

# --- Airflow DAG Definition ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_backdate_csv_to_csv',
    default_args=default_args,
    description='Daily incremental backdating ETL for Coffee Shop Sales',
    schedule='*/5 * * * *',   # Runs every 5 minutes
    catchup=False,
) as dag:
    daily_task = PythonOperator(
        task_id='run_daily_incremental',
        python_callable=run_daily_incremental,
    )

    daily_task
