import json
import os
import logging
from . import config

logger = logging.getLogger(__name__)

def load_checkpoint():
    """
    Loads the checkpoint state from disk.
    Returns a set of processed thread IDs.
    """
    if os.path.exists(config.CHECKPOINT_FILE):
        try:
            with open(config.CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
                return set(data.get('processed_threads', []))
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return set()
    return set()

def save_checkpoint(processed_threads):
    """
    Saves the list of processed thread IDs to disk.
    """
    try:
        data = {
            'processed_threads': list(processed_threads)
        }
        with open(config.CHECKPOINT_FILE, 'w') as f:
            json.dump(data, f)
        logger.info("Checkpoint saved.")
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")

def load_threads_list():
    """
    Loads the cached list of threads from disk.
    Returns a list of thread dictionaries or None if not found.
    """
    if os.path.exists(config.THREADS_LIST_FILE):
        try:
            with open(config.THREADS_LIST_FILE, 'r') as f:
                data = json.load(f)
                return data
        except Exception as e:
            logger.error(f"Failed to load threads list: {e}")
            return None
    return None

def save_threads_list(threads):
    """
    Saves the list of threads to disk.
    """
    try:
        with open(config.THREADS_LIST_FILE, 'w') as f:
            json.dump(threads, f)
        logger.info(f"Saved {len(threads)} threads to cache.")
    except Exception as e:
        logger.error(f"Failed to save threads list: {e}")
