import os
from datetime import datetime, timedelta
import pandas as pd
from etl_backdating_csv_to_csv import extract_data, transform_data, load_data

INPUT_FILE = "Coffee Shop Sales2.csv"
OUTPUT_FILE = "transformed.csv"

def get_next_date_to_process(output_csv, df_raw):
    """
    Determine the next date to backdate based on the last date in the CSV.
    If the CSV doesn't exist, return the earliest date in the raw data.
    """
    df_raw['transaction_date'] = pd.to_datetime(df_raw['transaction_date'], errors='coerce')
    
    if not os.path.exists(output_csv):
        return df_raw['transaction_date'].min()

    df_existing = pd.read_csv(output_csv)
    df_existing['transaction_date'] = pd.to_datetime(df_existing['transaction_date'], errors='coerce')
    df_existing = df_existing.dropna(subset=['transaction_date'])
    last_date = df_existing['transaction_date'].max()

    return last_date + timedelta(days=1)


def run_daily_incremental():
    """
    Run ETL for the next unprocessed date in the raw CSV.
    """
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

    # --- Transform and load ---
    df_transformed = transform_data(df_raw, start_date_str, end_date_str)
    if df_transformed.empty:
        print(f"⚠ No data found for {start_date_str}")
        return

    load_data(df_transformed, OUTPUT_FILE, start_date_str, end_date_str)
    print(f"✅ Daily backdate for {start_date_str} completed.")
