import pandas as pd
import glob
import os

data_dir = "data"
files = glob.glob(os.path.join(data_dir, "*.parquet"))

if not files:
    print("No parquet files found.")
else:
    latest_file = files[0]
    print(f"Reading {latest_file}...")
    df = pd.read_parquet(latest_file)
    
    print("\n--- Schema ---")
    print(df.info())
    
    print("\n--- First 3 Rows ---")
    print(df.head(3))
    
    print("\n--- Random Sample Text ---")
    if not df.empty:
        print(df.iloc[0]['raw_text'][:200])
