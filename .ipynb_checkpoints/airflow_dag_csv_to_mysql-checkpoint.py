import os
import pandas as pd
import mysql.connector
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# -----------------------------
# MySQL Config
# -----------------------------
MYSQL_HOST = "host.docker.internal"  # ✅ Connect to host machine MySQL
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "Whatsnew2711"
MYSQL_DB = "coffee_shop_sales"

# -----------------------------
# Helper Functions
# -----------------------------
def get_mysql_connection():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        return conn
    except mysql.connector.Error as err:
        logging.error(f"❌ Failed to connect to MySQL: {err}")
        raise  # fail the task if connection fails

def get_latest_date_from_mysql():
    logging.info("🔍 Checking latest date in MySQL...")
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(transaction_date) FROM sales_data;")
        latest_date = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        if latest_date:
            logging.info(f"✅ Latest date in DB: {latest_date}")
        else:
            logging.warning("⚠ No data in DB, starting from default date.")
        return latest_date
    except mysql.connector.Error as err:
        logging.error(f"❌ Failed to retrieve latest date: {err}")
        raise

def extract_data(input_csv):
    logging.info(f"📥 Extracting data from '{input_csv}'...")
    try:
        df = pd.read_csv(input_csv, sep=";")
        return df
    except FileNotFoundError:
        logging.error(f"❌ File not found: {input_csv}")
        return None

def transform_data(df, start_date_str, end_date_str):
    logging.info(f"🔄 Transforming data for {start_date_str} to {end_date_str}...")
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    df['transaction_time'] = pd.to_datetime(df['transaction_time'], errors='coerce').dt.strftime('%H:%M:%S')

    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)
    df_filtered = df[(df['transaction_date'] >= start_date) & (df['transaction_date'] <= end_date)].copy()

    if df_filtered.empty:
        logging.warning(f"⚠ No data found for {start_date_str}")
        return df_filtered

    # Map store locations
    location_map = {
        "Hell's Kitchen": "East Campus",
        "Astoria": "West Campus",
        "Lower Manhattan": "Main Campus"
    }
    df_filtered['store_location'] = df_filtered['store_location'].replace(location_map)

    # Convert unit_price
    df_filtered['unit_price'] = (df_filtered['unit_price'] * 15).round(2)

    # Convert key columns to string
    cols_to_convert = ['transaction_id', 'store_id', 'product_id']
    df_filtered[cols_to_convert] = df_filtered[cols_to_convert].apply(lambda col: col.dropna().astype(int).astype(str))

    # Force unique transaction IDs by appending date
    df_filtered['transaction_id'] = (
        df_filtered['transaction_id'].astype(str) + "_" +
        pd.to_datetime(df_filtered['transaction_date']).dt.strftime('%Y%m%d')
    )

    df_transformed = df_filtered.reindex(columns=[
        'transaction_id', 'transaction_date', 'transaction_time', 'transaction_qty',
        'store_id', 'store_location', 'product_id', 'unit_price',
        'product_category', 'product_type', 'product_detail'
    ], fill_value=None)

    return df_transformed

def load_data_to_mysql(df, table_name):
    if df.empty:
        logging.warning("⚠ No data to load to MySQL.")
        return

    logging.info("🚀 Loading data to MySQL...")
    conn = get_mysql_connection()
    cursor = conn.cursor()

    records = [tuple(x) for x in df.to_numpy()]

    insert_query = f"""
        INSERT INTO {table_name} (
            transaction_id, transaction_date, transaction_time, transaction_qty,
            store_id, store_location, product_id, unit_price,
            product_category, product_type, product_detail
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            transaction_date=VALUES(transaction_date),
            transaction_time=VALUES(transaction_time),
            transaction_qty=VALUES(transaction_qty),
            store_id=VALUES(store_id),
            store_location=VALUES(store_location),
            product_id=VALUES(product_id),
            unit_price=VALUES(unit_price),
            product_category=VALUES(product_category),
            product_type=VALUES(product_type),
            product_detail=VALUES(product_detail)
    """
    cursor.executemany(insert_query, records)
    conn.commit()
    logging.info(f"✅ Inserted/updated {cursor.rowcount} records.")

    cursor.close()
    conn.close()
    logging.info("MySQL connection closed.")

# -----------------------------
# ETL Runner
# -----------------------------
def run_daily_etl():
    input_file = os.path.join(os.path.dirname(__file__), "Coffee Shop Sales2.csv")
    table_name = "sales_data"

    latest_date_in_db = get_latest_date_from_mysql()
    if latest_date_in_db:
        date_to_process = (latest_date_in_db + timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        date_to_process = '2025-01-01'

    logging.info(f"📆 Processing date: {date_to_process}")

    df_raw = extract_data(input_file)
    if df_raw is not None:
        df_transformed = transform_data(df_raw, date_to_process, date_to_process)
        if df_transformed is not None:
            load_data_to_mysql(df_transformed, table_name)

# -----------------------------
# Airflow DAG
# -----------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_backdate_csv_to_mysql',
    default_args=default_args,
    description='Daily incremental update for the MySQL OLTP database',
    schedule='*/5 * * * *',
    catchup=False,
) as dag:
    run_update_task = PythonOperator(
        task_id='run_daily_update',
        python_callable=run_daily_etl,
    )

    run_update_task
