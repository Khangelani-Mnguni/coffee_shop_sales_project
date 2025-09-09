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

def extract_data_from_mysql(date_to_process):
    """
    Extracts daily transaction data from MySQL OLTP database
    for a specific date.
    """
    logging.info(f"📥 Extracting data from MySQL for {date_to_process}...")
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
        WHERE transaction_date = '{date_to_process}';
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
    by checking for existence before inserting to avoid duplicates.
    """
    logging.info("📤 Loading data to PostgreSQL...")
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
        for _, row in dim_date.iterrows():
            date_to_check = row["transaction_date"].date()
            if date_to_check in existing_dates:
                date_id_map[row["transaction_date"]] = existing_dates[date_to_check]
            else:
                cur.execute("""
                    INSERT INTO dim_date (transaction_date, day, month, year, weekday)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING date_id;
                """, (
                    date_to_check,
                    int(row["day"]),
                    int(row["month"]),
                    int(row["year"]),
                    row["weekday"]
                ))
                date_id_map[row["transaction_date"]] = cur.fetchone()[0]

        # -----------------------------
        # Dim Time (Optimized Check then Insert)
        # -----------------------------
        cur.execute("SELECT transaction_time, time_id FROM dim_time;")
        existing_times = {row[0]: row[1] for row in cur.fetchall()}
        time_id_map = {}
        for _, row in dim_time.iterrows():
            if row["transaction_time"] in existing_times:
                time_id_map[row["transaction_time"]] = existing_times[row["transaction_time"]]
            else:
                cur.execute("""
                    INSERT INTO dim_time (transaction_time, hour, minute, second)
                    VALUES (%s, %s, %s, %s)
                    RETURNING time_id;
                """, (
                    row["transaction_time"],
                    int(row["hour"]),
                    int(row["minute"]),
                    int(row["second"])
                ))
                time_id_map[row["transaction_time"]] = cur.fetchone()[0]

        # -----------------------------
        # Dim Store (Optimized Check then Insert)
        # -----------------------------
        cur.execute("SELECT store_location, store_id FROM dim_store;")
        existing_stores = {row[0]: row[1] for row in cur.fetchall()}
        store_id_map = {}
        for _, row in dim_store.iterrows():
            if row["store_location"] in existing_stores:
                store_id_map[row["store_id"]] = existing_stores[row["store_location"]]
            else:
                cur.execute("""
                    INSERT INTO dim_store (store_location)
                    VALUES (%s)
                    RETURNING store_id;
                """, (row["store_location"],))
                store_id_map[row["store_id"]] = cur.fetchone()[0]

        # -----------------------------
        # Dim Product (Optimized Check then Insert)
        # -----------------------------
        cur.execute("""
            SELECT product_category, product_type, product_detail, product_id
            FROM dim_product;
        """)
        existing_products = {(row[0], row[1], row[2]): row[3] for row in cur.fetchall()}
        product_id_map = {}
        for _, row in dim_product.iterrows():
            product_tuple = (row["product_category"], row["product_type"], row["product_detail"])
            if product_tuple in existing_products:
                product_id_map[row["product_id"]] = existing_products[product_tuple]
            else:
                cur.execute("""
                    INSERT INTO dim_product (product_category, product_type, product_detail, unit_price)
                    VALUES (%s, %s, %s, %s)
                    RETURNING product_id;
                """, (
                    row["product_category"],
                    row["product_type"],
                    row["product_detail"],
                    float(row["unit_price"])
                ))
                product_id_map[row["product_id"]] = cur.fetchone()[0]

        conn.commit()

        # -----------------------------
        # Fact Table (Optimized Check then Insert)
        # -----------------------------
        cur.execute("SELECT transaction_id FROM fact_sales;")
        existing_transactions = {row[0] for row in cur.fetchall()}
        
        for _, row in df_fact.iterrows():
            if row["transaction_id"] not in existing_transactions:
                cur.execute("""
                    INSERT INTO fact_sales (transaction_id, date_id, time_id, store_id, product_id, transaction_qty, total_amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (
                    row["transaction_id"],
                    date_id_map[row["transaction_date"]],
                    time_id_map[row["transaction_time"]],
                    store_id_map[row["store_id"]],
                    product_id_map[row["product_id"]],
                    int(row["transaction_qty"]),
                    float(row["total_amount"])
                ))
        
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
        # Convert string dates to datetime.date objects for easier iteration
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        logging.error("❌ Invalid date format. Please use YYYY-MM-DD.")
        return

    logging.info(f"⏳ Starting backdating process from {start_date} to {end_date}...")

    current_date = start_date
    while current_date <= end_date:
        date_to_process = current_date.strftime('%Y-%m-%d')
        logging.info(f"📆 Processing date: {date_to_process}")

        # --- Extract ---
        df_raw = extract_data_from_mysql(date_to_process)
        
        # Check if extraction was successful and data exists
        if df_raw is not None and not df_raw.empty:
            # --- Transform ---
            try:
                dim_date, dim_time, dim_store, dim_product, df_fact = transform_data(df_raw)
                # --- Load ---
                load_data_to_postgres(dim_date, dim_time, dim_store, dim_product, df_fact)
                logging.info(f"✅ Completed ETL for: {date_to_process}")
            except Exception as e:
                logging.error(f"❌ ETL process failed for {date_to_process}: {e}")
        else:
            logging.warning(f"⚠️ No data found for {date_to_process} or extraction failed. Skipping.")
        
        # Increment to the next day
        current_date += timedelta(days=1)
    
    logging.info("🎉 Backdating process finished.")

if __name__ == "__main__":
    start_input = input("Enter start date (YYYY-MM-DD): ").strip()
    end_input = input("Enter end date (YYYY-MM-DD): ").strip()
    run_backdate(start_input, end_input)