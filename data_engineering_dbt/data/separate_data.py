import pandas as pd
import os

def split_csv_by_store(input_filepath, split_column='store_id'):
    """
    Reads a CSV and splits it into multiple CSVs based on unique values in a specified column.
    Saves the new files in the current working directory.
    """
    try:
        # 1. Load the dataset with the correct semicolon separator
        print(f"Loading data from '{input_filepath}'...")
        df = pd.read_csv(input_filepath, sep=';')
        
        # Clean up column names just in case there are trailing spaces
        df.columns = df.columns.str.strip()
        
        # 2. Verify the target column exists
        if split_column not in df.columns:
            print(f"Error: The column '{split_column}' was not found in the CSV.")
            print(f"Available columns: {', '.join(df.columns)}")
            return
            
        # 3. Get a list of all unique values in the target column
        unique_stores = df[split_column].dropna().unique()
        print(f"Found {len(unique_stores)} unique stores: {unique_stores}")
        
        # 4. Iterate through each store, filter the data, and save it
        for store in unique_stores:
            # Filter the dataframe for the current store
            store_data = df[df[split_column] == store]
            
            # Sanitize the store name to create a valid filename 
            safe_filename = str(store).replace(' ', '_').replace('/', '_').replace("'", "")
            output_filename = f"{safe_filename}.csv"
            
            # Export to CSV without the index column in the current directory
            # If you want the outputs to ALSO use semicolons, change this to: store_data.to_csv(output_filename, index=False, sep=';')
            store_data.to_csv(output_filename, index=False)
            print(f" -> Saved {len(store_data)} records to '{output_filename}'")
            
        print("Data splitting complete!")
        
    except FileNotFoundError:
        print(f"Error: Could not find the file at '{input_filepath}'. Please check the path and try again.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# ==========================================
# Run the script
# ==========================================
if __name__ == "__main__":
    # Updated with your specific file path and name
    INPUT_CSV = '/Users/mac/Desktop/Coffee Shop Sales2.csv'       
    
    # You can change this to 'store_location' if you prefer text names for the files
    TARGET_COLUMN = 'store_location'     
    
    # Run the function
    split_csv_by_store(INPUT_CSV, TARGET_COLUMN)