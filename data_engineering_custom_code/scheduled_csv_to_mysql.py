import pandas as pd
import os
import mysql.connector
import logging
from datetime import datetime, timedelta

# Set up logging for better visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =========================================================
# 1️⃣ Database Connection Configurations
# =========================================================
# Configuration for the MySQL OLTP database.
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "Whatsnew2711"
MYSQL_DB = 'coffee_shop_sales'

def get_mysql_connection():
    """Establishes and returns a MySQL database connection."""
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
        return None

# =========================================================
# 2️⃣ ETL Functions
# =========================================================

def extract_data(input_csv):
    """
    Extracts data from a specified CSV file into a pandas DataFrame.
    """
    logging.info(f"📥 Extracting data from '{input_csv}'...")
    try:
        df = pd.read_csv(input_csv, sep=";")
        return df
    except FileNotFoundError:
        logging.error(f"❌ Error: The file '{input_csv}' was not found.")
        return None

def transform_data(df, start_offset_date, end_offset_date):
    """
    Transforms the raw DataFrame by:
    - Filtering by date range
    - Formatting dates & times
    - Mapping store locations
    - Converting currency
    - Regenerating unique transaction IDs
    """
    logging.info("🔄 Transforming data...")

    # Convert date and time
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    df['transaction_time'] = pd.to_datetime(
        df['transaction_time'], format='%H:%M:%S', errors='coerce'
    ).dt.strftime('%H:%M:%S')

    # Filter by date range
    start_date = pd.to_datetime(start_offset_date)
    end_date = pd.to_datetime(end_offset_date)
    df_filtered = df[
        (df['transaction_date'] >= start_date) & (df['transaction_date'] <= end_date)
    ].copy()

    if df_filtered.empty:
        logging.warning(f"⚠ No data found for the range {start_offset_date} to {end_offset_date}.")
        return df_filtered

    # Format date for output
    df_filtered['transaction_date'] = df_filtered['transaction_date'].dt.strftime('%Y-%m-%d')

    # Map store locations
    location_map = {
        "Hell's Kitchen": "East Campus",
        "Astoria": "West Campus",
        "Lower Manhattan": "Main Campus"
    }
    df_filtered['store_location'] = df_filtered['store_location'].replace(location_map)

    # Convert unit_price
    df_filtered['unit_price'] = (df_filtered['unit_price'] * 15).round(2)

    # Convert key columns to string safely
    cols_to_convert = ['transaction_id', 'store_id', 'product_id']
    df_filtered[cols_to_convert] = df_filtered[cols_to_convert].apply(
        lambda col: col.dropna().astype(int).astype(str)
    )

    # Force unique transaction IDs by appending the date
    df_filtered['transaction_id'] = (
        df_filtered['transaction_id'].astype(str) + "_" +
        pd.to_datetime(df_filtered['transaction_date']).dt.strftime('%Y%m%d')
    )
    
    # Reorder columns to match the MySQL table structure
    df_transformed = df_filtered.reindex(columns=[
        'transaction_id', 'transaction_date', 'transaction_time', 'transaction_qty',
        'store_id', 'store_location', 'product_id', 'unit_price',
        'product_category', 'product_type', 'product_detail'
    ], fill_value=None)

    return df_transformed


def load_data_to_mysql(df, table_name):
    """
    Loads transformed data into a MySQL table using the ON DUPLICATE KEY UPDATE.
    """
    if df.empty:
        logging.warning("⚠ No data to load to MySQL.")
        return

    logging.info("🚀 Loading data to MySQL...")
    conn = None
    cursor = None
    try:
        conn = get_mysql_connection()
        if not conn:
            return
        
        cursor = conn.cursor()
        logging.info("Successfully connected to MySQL database.")

        # Prepare the list of tuples for insertion
        records = [tuple(x) for x in df.to_numpy()]

        # The INSERT query with ON DUPLICATE KEY UPDATE to handle idempotency
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
        logging.info(f"✅ Load complete! Inserted/updated {cursor.rowcount} records.")

    except mysql.connector.Error as err:
        logging.error(f"❌ MySQL Error: {err}")
        if conn and conn.is_connected():
            conn.rollback()
    finally:
        if cursor is not None:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            logging.info("MySQL connection closed.")

def get_latest_date_from_mysql():
    """
    Queries MySQL to find the most recent transaction date in the
    sales_data table.
    """
    logging.info("🔍 Checking for the latest date in MySQL...")
    conn = None
    cursor = None
    try:
        conn = get_mysql_connection()
        if not conn:
            return None
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT MAX(transaction_date)
            FROM sales_data;
        """)
        latest_date = cursor.fetchone()[0]
        
        if latest_date:
            logging.info(f"✅ Latest date found in database: {latest_date}")
            return latest_date
        else:
            logging.warning("⚠️ No data found in sales_data table. Starting from default date.")
            return None
    except mysql.connector.Error as err:
        logging.error(f"❌ Failed to retrieve latest date from MySQL: {err}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# =========================================================
# 3️⃣ Main Execution Logic for Daily Schedule
# =========================================================
if __name__ == "__main__":
    input_file = "Coffee Shop Sales2.csv"
    table_name = "sales_data"
    
    # Get the latest date from the database
    latest_date_in_db = get_latest_date_from_mysql()
    
    # Determine the date to process. If no data exists, start from the beginning.
    if latest_date_in_db:
        # Load the next day's data
        date_to_process = (latest_date_in_db + timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        # Default start date for the backfilling.
        date_to_process = '2025-01-01'

    logging.info(f"📆 Processing date: {date_to_process}")

    df_raw = extract_data(input_file)
    if df_raw is not None:
        # The start and end date for transformation is the same, as we process one day at a time
        df_transformed = transform_data(df_raw, date_to_process, date_to_process)
        if df_transformed is not None:
            load_data_to_mysql(df_transformed, table_name)
