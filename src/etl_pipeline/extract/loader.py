import pandas as pd
import os
import glob
import logging
from . import config

logger = logging.getLogger(__name__)

def get_existing_ids():
    """
    Scans the data directory for existing files and returns a set of processed IDs.
    """
    # Check for both parquet and csv to be safe
    all_files = glob.glob(os.path.join(config.DATA_DIR, "hn_jobs_raw_*.parquet"))
    all_files.extend(glob.glob(os.path.join(config.DATA_DIR, "hn_jobs_raw_*.csv")))
    
    existing_ids = set()
    for f in all_files:
        try:
            if f.endswith('.parquet'):
                df = pd.read_parquet(f, columns=['id'])
            else:
                df = pd.read_csv(f, usecols=['id'])
            existing_ids.update(df['id'].astype(str).tolist())
        except Exception as e:
            logger.error(f"Error reading {f}: {e}")
            
    return existing_ids

def save_data(df):
    """
    Saves the dataframe to disk.
    Determines filename based on min/max thread_date in the data.
    """
    if df.empty:
        logger.info("No data to save.")
        return
        
    # Ensure IDs are strings for consistency
    df['id'] = df['id'].astype(str)
    
    # Sort by date
    df = df.sort_values('thread_date')
    
    start_date = df['thread_date'].min()
    end_date = df['thread_date'].max()
    
    # Using parquet as requested for performance
    filename = f"hn_jobs_raw_{start_date}_{end_date}.parquet"
    filepath = os.path.join(config.DATA_DIR, filename)
    
    try:
        df.to_parquet(filepath, index=False)
        logger.info(f"Saved {len(df)} records to {filepath}")
    except Exception as e:
        logger.error(f"Failed to save data to {filepath}: {e}")

def save_thread_data(df, thread_id):
    """
    Saves data for a single thread to a parquet file.
    """
    if df.empty:
        return

    filename = f"thread_{thread_id}.parquet"
    filepath = os.path.join(config.THREADS_DIR, filename)
    
    try:
        # Ensure IDs are strings
        df['id'] = df['id'].astype(str)
        df.to_parquet(filepath, index=False)
        logger.info(f"Saved thread {thread_id} data to {filepath}")
    except Exception as e:
        logger.error(f"Failed to save thread data to {filepath}: {e}")

def merge_thread_files():
    """
    Merges all thread parquet files into the main dataset.
    """
    thread_files = glob.glob(os.path.join(config.THREADS_DIR, "*.parquet"))
    if not thread_files:
        logger.info("No thread files to merge.")
        return

    dfs = []
    for f in thread_files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as e:
            logger.error(f"Error reading {f}: {e}")
    
    if dfs:
        full_df = pd.concat(dfs, ignore_index=True)
        # Deduplicate just in case
        full_df.drop_duplicates(subset=['id'], inplace=True)
        save_data(full_df)
