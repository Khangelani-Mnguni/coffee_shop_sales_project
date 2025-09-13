import pandas as pd
import mysql.connector
import psycopg2
from psycopg2 import extras
import logging
from datetime import datetime, timedelta

# Set up logging for visibility and easy debugging.
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

# Configuration for the PostgreSQL OLAP database.
PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "Whatsnew2711"
PG_DB = "coffee_sales_data"

def get_postgres_connection():
    """Establishes and returns a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DB
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"❌ Failed to connect to PostgreSQL: {e}")
        return None

# =========================================================
# 2️⃣ ETL Functions
# =========================================================

def extract_data_from_mysql(start_date_to_process, end_date_to_process):
    """
    Extracts transaction data from MySQL OLTP database
    for a specific date range.
    """
    logging.info(f"📥 Extracting data from MySQL for date range {start_date_to_process} to {end_date_to_process}...")
    conn = None
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        query = f"""
        SELECT
            transaction_id,
            transaction_date,
            transaction_time,
            transaction_qty,
            store_id,
            store_location,
            product_id,
            unit_price,
            product_category,
            product_type,
            product_detail
        FROM sales_data
        WHERE transaction_date BETWEEN '{start_date_to_process}' AND '{end_date_to_process}';
        """
        df_raw = pd.read_sql(query, conn)
        logging.info(f"✅ Extracted {len(df_raw)} records.")
        return df_raw
    except mysql.connector.Error as err:
        logging.error(f"❌ MySQL Error: {err}")
        return None
    finally:
        if conn and conn.is_connected():
            conn.close()
            logging.info("MySQL connection closed.")

def transform_data(df_raw):
    """
    Transforms raw MySQL data into star schema DataFrames that match PostgreSQL tables.
    Returns: dim_date, dim_time, dim_store, dim_product, df_fact
    """
    logging.info("🔄 Transforming data for OLAP...")

    # Convert transaction_date and time to datetime
    df_raw["transaction_date"] = pd.to_datetime(df_raw["transaction_date"], errors="coerce")
    if pd.api.types.is_timedelta64_dtype(df_raw["transaction_time"]):
        df_raw["transaction_time"] = df_raw["transaction_time"].apply(
            lambda x: (pd.Timestamp("00:00:00") + x).time() if pd.notnull(x) else None
        )
    else:
        df_raw["transaction_time"] = pd.to_datetime(
            df_raw["transaction_time"].astype(str), errors="coerce"
        ).dt.time

    # Drop rows where conversion failed
    df_raw = df_raw.dropna(subset=["transaction_date", "transaction_time"])

    # Calculate total_amount
    df_raw["total_amount"] = df_raw["transaction_qty"] * df_raw["unit_price"]

    # Dim Date
    dim_date = df_raw[["transaction_date"]].drop_duplicates().reset_index(drop=True)
    dim_date["day"] = dim_date["transaction_date"].dt.day
    dim_date["month"] = dim_date["transaction_date"].dt.month
    dim_date["year"] = dim_date["transaction_date"].dt.year
    dim_date["weekday"] = dim_date["transaction_date"].dt.day_name()

    # Dim Time
    dim_time = df_raw[["transaction_time"]].drop_duplicates().reset_index(drop=True)
    dim_time["hour"] = dim_time["transaction_time"].apply(lambda x: x.hour)
    dim_time["minute"] = dim_time["transaction_time"].apply(lambda x: x.minute)
    dim_time["second"] = dim_time["transaction_time"].apply(lambda x: x.second)

    # Dim Store
    dim_store = df_raw[["store_id", "store_location"]].drop_duplicates().reset_index(drop=True)

    # Dim Product
    dim_product = df_raw[["product_id", "product_category", "product_type", "product_detail", "unit_price"]].drop_duplicates().reset_index(drop=True)

    # Fact Table
    df_fact = df_raw.copy()
    df_fact = df_fact.merge(dim_date, on="transaction_date")
    df_fact = df_fact.merge(dim_time, on="transaction_time")
    df_fact = df_fact.merge(dim_store, on=["store_id", "store_location"])
    df_fact = df_fact.merge(dim_product, on=["product_id", "product_category", "product_type", "product_detail", "unit_price"])

    df_fact = df_fact[[
        "transaction_id",
        "transaction_date",
        "transaction_time",
        "store_id",
        "product_id",
        "transaction_qty",
        "total_amount"
    ]]

    logging.info(f"✅ Transformation completed. Records: {len(df_fact)}")
    return dim_date, dim_time, dim_store, dim_product, df_fact

def load_data_to_postgres(dim_date, dim_time, dim_store, dim_product, df_fact):
    """
    Loads transformed data into PostgreSQL OLAP database
    using batch insertion for better performance.
    """
    logging.info("📤 Loading data to PostgreSQL using batch insertion...")
    conn = None
    try:
        conn = get_postgres_connection()
        if not conn:
            return

        cur = conn.cursor()

        # -----------------------------
        # Dim Date (Optimized Check then Insert)
        # -----------------------------
        cur.execute("SELECT transaction_date, date_id FROM dim_date;")
        existing_dates = {row[0]: row[1] for row in cur.fetchall()}
        date_id_map = {}
        dates_to_insert = []
        for _, row in dim_date.iterrows():
            date_to_check = row["transaction_date"].date()
            if date_to_check in existing_dates:
                date_id_map[row["transaction_date"]] = existing_dates[date_to_check]
            else:
                dates_to_insert.append((
                    date_to_check,
                    int(row["day"]),
                    int(row["month"]),
                    int(row["year"]),
                    row["weekday"]
                ))
        
        if dates_to_insert:
            insert_query = """
                INSERT INTO dim_date (transaction_date, day, month, year, weekday)
                VALUES %s RETURNING date_id, transaction_date;
            """
            psycopg2.extras.execute_values(cur, insert_query, dates_to_insert, template=None)
            new_dates = cur.fetchall()
            for date_id, transaction_date in new_dates:
                # Find the corresponding original transaction_date from dim_date DataFrame
                original_date = dim_date[dim_date['transaction_date'].dt.date == transaction_date]['transaction_date'].iloc[0]
                date_id_map[original_date] = date_id
        
        # -----------------------------
        # Dim Time (Optimized Check then Insert)
        # -----------------------------
        cur.execute("SELECT transaction_time, time_id FROM dim_time;")
        existing_times = {row[0]: row[1] for row in cur.fetchall()}
        time_id_map = {}
        times_to_insert = []
        for _, row in dim_time.iterrows():
            if row["transaction_time"] in existing_times:
                time_id_map[row["transaction_time"]] = existing_times[row["transaction_time"]]
            else:
                times_to_insert.append((
                    row["transaction_time"],
                    int(row["hour"]),
                    int(row["minute"]),
                    int(row["second"])
                ))

        if times_to_insert:
            insert_query = """
                INSERT INTO dim_time (transaction_time, hour, minute, second)
                VALUES %s RETURNING time_id, transaction_time;
            """
            psycopg2.extras.execute_values(cur, insert_query, times_to_insert, template=None)
            new_times = cur.fetchall()
            for time_id, transaction_time in new_times:
                time_id_map[transaction_time] = time_id

        # -----------------------------
        # Dim Store (Optimized Check then Insert)
        # -----------------------------
        cur.execute("SELECT store_location, store_id FROM dim_store;")
        existing_stores = {row[0]: row[1] for row in cur.fetchall()}
        store_id_map = {}
        stores_to_insert = []
        for _, row in dim_store.iterrows():
            if row["store_location"] in existing_stores:
                store_id_map[row["store_id"]] = existing_stores[row["store_location"]]
            else:
                stores_to_insert.append((row["store_location"],))
                store_id_map[row["store_id"]] = None  # Placeholder to be updated after insert

        if stores_to_insert:
            insert_query = "INSERT INTO dim_store (store_location) VALUES %s RETURNING store_id, store_location;"
            psycopg2.extras.execute_values(cur, insert_query, stores_to_insert, template=None)
            new_stores = cur.fetchall()
            for store_id, store_location in new_stores:
                # Find the original store_id by store_location and update map
                original_store_id = dim_store[dim_store['store_location'] == store_location]['store_id'].iloc[0]
                store_id_map[original_store_id] = store_id
        
        # -----------------------------
        # Dim Product (Optimized Check then Insert)
        # -----------------------------
        cur.execute("""
            SELECT product_category, product_type, product_detail, product_id
            FROM dim_product;
        """)
        existing_products = {(row[0], row[1], row[2]): row[3] for row in cur.fetchall()}
        product_id_map = {}
        products_to_insert = []
        for _, row in dim_product.iterrows():
            product_tuple = (row["product_category"], row["product_type"], row["product_detail"])
            if product_tuple in existing_products:
                product_id_map[row["product_id"]] = existing_products[product_tuple]
            else:
                products_to_insert.append((
                    row["product_category"],
                    row["product_type"],
                    row["product_detail"],
                    float(row["unit_price"])
                ))
                product_id_map[row["product_id"]] = None # Placeholder

        if products_to_insert:
            insert_query = """
                INSERT INTO dim_product (product_category, product_type, product_detail, unit_price)
                VALUES %s RETURNING product_id, product_category, product_type, product_detail;
            """
            psycopg2.extras.execute_values(cur, insert_query, products_to_insert, template=None)
            new_products = cur.fetchall()
            for product_id, p_cat, p_type, p_detail in new_products:
                original_product_id = dim_product[(dim_product['product_category'] == p_cat) &
                                                 (dim_product['product_type'] == p_type) &
                                                 (dim_product['product_detail'] == p_detail)]['product_id'].iloc[0]
                product_id_map[original_product_id] = product_id

        # -----------------------------
        # Fact Table (Batch Insert)
        # -----------------------------
        logging.info("Batch inserting into fact_sales table...")

        # First, fetch all existing transaction_ids to avoid duplicates
        cur.execute("SELECT transaction_id FROM fact_sales;")
        existing_transactions = {row[0] for row in cur.fetchall()}
        
        # Filter out transactions that already exist
        df_fact_to_insert = df_fact[~df_fact['transaction_id'].isin(existing_transactions)].copy()

        if df_fact_to_insert.empty:
            logging.info("⚠️ No new transactions to insert. Skipping batch insertion.")
            conn.commit()
            cur.close()
            return

        # Ensure all IDs are mapped before preparing fact data
        df_fact_to_insert['date_id'] = df_fact_to_insert['transaction_date'].map(date_id_map)
        df_fact_to_insert['time_id'] = df_fact_to_insert['transaction_time'].map(time_id_map)
        df_fact_to_insert['store_id'] = df_fact_to_insert['store_id'].map(store_id_map)
        df_fact_to_insert['product_id'] = df_fact_to_insert['product_id'].map(product_id_map)
        
        # Drop any rows where a key was not found (shouldn't happen with correct logic)
        df_fact_to_insert.dropna(subset=['date_id', 'time_id', 'store_id', 'product_id'], inplace=True)

        fact_data_to_insert = [
            (
                row["transaction_id"],
                int(row["date_id"]),
                int(row["time_id"]),
                int(row["store_id"]),
                int(row["product_id"]),
                int(row["transaction_qty"]),
                float(row["total_amount"])
            )
            for _, row in df_fact_to_insert.iterrows()
        ]

        # Use execute_values for efficient batch insertion
        if fact_data_to_insert:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO fact_sales (transaction_id, date_id, time_id, store_id, product_id, transaction_qty, total_amount) VALUES %s",
                fact_data_to_insert,
                page_size=5000
            )

        conn.commit()
        cur.close()
        logging.info("✅ Data loaded to PostgreSQL successfully!")

    except Exception as e:
        logging.error(f"❌ Failed to load data to PostgreSQL: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# =========================================================
# 3️⃣ Main Execution Logic for Backdating
# =========================================================

def run_backdate(start_date_str, end_date_str):
    """
    Runs the full ETL pipeline for a specified date range.
    """
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        logging.error("❌ Invalid date format. Please use YYYY-MM-DD.")
        return

    logging.info(f"⏳ Starting backdating process from {start_date} to {end_date}...")

    # --- Extract all data for the date range at once ---
    df_raw = extract_data_from_mysql(start_date, end_date)
    
    if df_raw is not None and not df_raw.empty:
        # --- Transform all data at once ---
        try:
            dim_date, dim_time, dim_store, dim_product, df_fact = transform_data(df_raw)
            # --- Load all data in batches ---
            load_data_to_postgres(dim_date, dim_time, dim_store, dim_product, df_fact)
            logging.info(f"✅ Completed ETL for date range: {start_date} to {end_date}")
        except Exception as e:
            logging.error(f"❌ ETL process failed for date range: {start_date} to {end_date}: {e}")
    else:
        logging.warning(f"⚠️ No data found for the date range {start_date} to {end_date} or extraction failed. Skipping.")
    
    logging.info("🎉 Backdating process finished.")

if __name__ == "__main__":
    start_input = input("Enter start date (YYYY-MM-DD): ").strip()
    end_input = input("Enter end date (YYYY-MM-DD): ").strip()
    run_backdate(start_input, end_input)