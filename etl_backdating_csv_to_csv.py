import pandas as pd
import os
from datetime import datetime

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

    return df_filtered

def load_data(df_transformed, output_csv, start_offset_date, end_offset_date):
    """
    Loads transformed data into the CSV, replacing rows in the given date range
    and ensuring all transaction IDs are unique across the file.
    """
    if df_transformed.empty:
        print("⚠ No data to load.")
        return

    # Ensure proper types
    df_transformed['transaction_date'] = pd.to_datetime(df_transformed['transaction_date'])
    df_transformed['transaction_id'] = df_transformed['transaction_id'].astype(str)

    if os.path.exists(output_csv):
        print("📂 Existing data found. Removing old data in the same date range...")
        df_existing = pd.read_csv(output_csv)
        df_existing['transaction_date'] = pd.to_datetime(df_existing['transaction_date'])
        df_existing['transaction_id'] = df_existing['transaction_id'].astype(str)

        # Remove rows from existing file that fall within the same date range
        mask = ~(
            (df_existing['transaction_date'] >= pd.to_datetime(start_offset_date)) &
            (df_existing['transaction_date'] <= pd.to_datetime(end_offset_date))
        )
        df_existing = df_existing[mask]

        # Append new transformed data
        df_final = pd.concat([df_existing, df_transformed], ignore_index=True)
    else:
        print("🆕 No existing data. Creating a new file...")
        df_final = df_transformed.copy()

    # Keep only unique transaction IDs (final safeguard)
    df_final = df_final.drop_duplicates(subset=['transaction_id'], keep='last')
    df_final = df_final.sort_values(by=['transaction_date', 'transaction_time'])

    df_final.to_csv(output_csv, index=False)
    print(f"✅ Load complete! Total unique transactions: {df_final['transaction_id'].nunique()}")

if __name__ == "__main__":
    input_file = "Coffee Shop Sales2.csv"  # your raw CSV file
    output_file = "processed_sales_data.csv"  # final output

    start_date = input("Enter start date (YYYY-MM-DD): ").strip()
    end_date = input("Enter end date (YYYY-MM-DD): ").strip()

    df_raw = extract_data(input_file)
    if df_raw is not None:
        df_transformed = transform_data(df_raw, start_date, end_date)
        load_data(df_transformed, output_file, start_date, end_date)
