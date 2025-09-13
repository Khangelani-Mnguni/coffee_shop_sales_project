import pandas as pd
import os
import mysql.connector
import logging
import random
from datetime import datetime, timedelta
from faker import Faker

# Set up logging for better visibility
logging.basicConfig(level=logging.INFO)
fake = Faker()

def generate_synthetic_data(start_date, end_date, num_records_per_day):
    """
    Generates synthetic coffee shop sales data within a specified date range.
    """
    print("✨ Generating synthetic data...")
    
    # Define product categories and their corresponding types and details
    product_data = {
        'Coffee': {
            'Gourmet brewed coffee': ['Ethiopia Rg', 'Colombian Sm', 'Brazilian Lg'],
            'Drip coffee': ['Our Old Time Diner Blend Sm', 'Our Old Time Diner Blend Lg'],
            'Organic brewed coffee': ['Brazilian Rg', 'Peruvian Lg'],
            'Premium brewed coffee': ['Jamaican Coffee River Sm', 'Blue Mountain Rg'],
            'Espresso': ['Americano Rg', 'Latte Lg', 'Cappuccino Sm']
        },
        'Tea': {
            'Brewed Chai tea': ['Spicy Eye Opener Chai Lg', 'Morning Sunrise Chai Lg'],
            'Brewed Black tea': ['Earl Grey Rg', 'English Breakfast Sm'],
            'Brewed Herbal tea': ['Chamomile Lg', 'Peppermint Rg']
        },
        'Drinking Chocolate': {
            'Hot chocolate': ['Dark chocolate Lg', 'Caramel Lg'],
            'Flavored hot chocolate': ['White chocolate Sm', 'Hazelnut Rg']
        },
        'Bakery': {
            'Scone': ['Oatmeal Scone', 'Cranberry Scone'],
            'Pastry': ['Croissant', 'Apple Danish'],
            'Muffin': ['Blueberry Muffin', 'Chocolate Chip Muffin']
        }
    }

    # Define store locations and their IDs
    store_locations = {
        'Lower Manhattan': 5,
        'Astoria': 3,
        "Hell's Kitchen": 8
    }

    # Generate a list of dates
    date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    
    records = []
    transaction_id_counter = 1

    for date in date_list:
        # Simulate varying number of transactions per day
        num_transactions = random.randint(num_records_per_day // 2, num_records_per_day * 2)
        
        for _ in range(num_transactions):
            transaction_time = fake.time_object().strftime('%H:%M:%S')
            transaction_qty = random.randint(1, 3)
            store_location = random.choice(list(store_locations.keys()))
            store_id = store_locations[store_location]
            
            product_category = random.choice(list(product_data.keys()))
            product_type = random.choice(list(product_data[product_category].keys()))
            product_detail = random.choice(product_data[product_category][product_type])
            
            # Unit price based on product category and size
            if 'Sm' in product_detail:
                unit_price = round(random.uniform(2.0, 3.0), 2)
            elif 'Rg' in product_detail:
                unit_price = round(random.uniform(2.5, 4.0), 2)
            elif 'Lg' in product_detail:
                unit_price = round(random.uniform(3.0, 5.0), 2)
            else:
                unit_price = round(random.uniform(2.0, 5.0), 2)

            # Assign product ID
            product_id = random.randint(1, 100)
            
            records.append([
                transaction_id_counter,
                date.strftime('%Y-%m-%d'),
                transaction_time,
                transaction_qty,
                store_id,
                store_location,
                product_id,
                unit_price,
                product_category,
                product_type,
                product_detail
            ])
            transaction_id_counter += 1

    df = pd.DataFrame(records, columns=[
        'transaction_id', 'transaction_date', 'transaction_time', 'transaction_qty',
        'store_id', 'store_location', 'product_id', 'unit_price',
        'product_category', 'product_type', 'product_detail'
    ])

    print(f"✅ Data generation complete! Generated {len(df)} records.")
    return df

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


def load_data_to_mysql(df, db_config, table_name, batch_size=5000):
    """
    Loads transformed data into a MySQL table in batches.
    """
    if df.empty:
        print("⚠ No data to load to MySQL.")
        return

    print("🚀 Loading data to MySQL...")
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        print("Successfully connected to MySQL database.")

        records = [tuple(x) for x in df.to_numpy()]
        num_records = len(records)

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

        # Batch loading
        for i in range(0, num_records, batch_size):
            batch = records[i:i + batch_size]
            print(f"📦 Loading batch {i // batch_size + 1} of {num_records // batch_size + 1}...")
            cursor.executemany(insert_query, batch)
            conn.commit()

        print(f"✅ Load complete! Inserted/updated {num_records} records.")

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
    # --- Configuration ---
    table_name = "sales_data"
    num_records_per_day = 500  # Adjust to generate more or fewer records
    
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'Whatsnew2711',
        'database': 'coffee_shop_sales'
    }

    start_date_str = input("Enter start offset date: ")
    end_date_str = input("Enter end offset date: ")

    # --- Execution Flow ---
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    
    df_generated = generate_synthetic_data(start_date, end_date, num_records_per_day)

    if not df_generated.empty:
        df_transformed = transform_data(df_generated, start_date_str, end_date_str)
        if df_transformed is not None:
            load_data_to_mysql(df_transformed, db_config, table_name)
