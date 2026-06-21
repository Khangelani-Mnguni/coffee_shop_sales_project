# scheduled_oltp_update.py
import pandas as pd
import os
from datetime import datetime
import mysql.connector
import logging

# Set up logging for better visibility
logging.basicConfig(level=logging.INFO)

def extract_data(input_csv):
    """
    Extracts data from a specified CSV file into a pandas DataFrame.
    """
    print(f"📥 Extracting data from '{input_csv}'...")
    try:
        df = pd.read_csv(input_csv, sep=";")
        return df
    except FileNotFoundError:
        print(f"❌ Error: The file '{input_csv}' was not found.")
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
    print("🔄 Transforming data...")

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
        print(f"⚠ No data found for the range {start_offset_date} to {end_offset_date}.")
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


def load_data_to_mysql(df, db_config, table_name):
    """
    Loads transformed data into a MySQL table.
    """
    if df.empty:
        print("⚠ No data to load to MySQL.")
        return

    print("🚀 Loading data to MySQL...")
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        print("Successfully connected to MySQL database.")

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
        print(f"✅ Load complete! Inserted/updated {cursor.rowcount} records.")

    except mysql.connector.Error as err:
        logging.error(f"❌ MySQL Error: {err}")
        if 'conn' in locals() and conn.is_connected():
            conn.rollback()
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
        print("MySQL connection closed.")

if __name__ == "__main__":
    input_file = "Coffee Shop Sales2.csv"
    table_name = "sales_data"
    
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'Whatsnew2711',
        'database': 'coffee_shop_sales'
    }

    start_date = input("Enter start date (YYYY-MM-DD): ").strip()
    end_date = input("Enter end date (YYYY-MM-DD): ").strip()

    df_raw = extract_data(input_file)
    if df_raw is not None:
        df_transformed = transform_data(df_raw, start_date, end_date)
        if df_transformed is not None:
            load_data_to_mysql(df_transformed, db_config, table_name)
