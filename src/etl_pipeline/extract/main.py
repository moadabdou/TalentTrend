import logging
import pandas as pd
import time
import sys
from datetime import datetime, timedelta
from requests.exceptions import RequestException
from . import config, fetcher, parser, loader, checkpoint_manager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    hn_fetcher = fetcher.HNFetcher()
    
    # 1. Load Checkpoint
    processed_threads = checkpoint_manager.load_checkpoint()
    logger.info(f"Resuming with {len(processed_threads)} threads already processed.")
    
    try:
        # 2. Fetch 'Who is hiring' threads
        all_threads = checkpoint_manager.load_threads_list()
        
        if all_threads:
            logger.info(f"Loaded {len(all_threads)} threads from cache.")
        else:
            logger.info("No threads cache found. Fetching from Hacker News...")
            all_threads = []
            page = 1
            MAX_SUBMISSION_PAGES = 15 # Increased to cover 2020-2025
            
            consecutive_errors = 0
            MAX_CONSECUTIVE_ERRORS = config.MAX_RETRIES
            
            next_page_params = None

            while page <= MAX_SUBMISSION_PAGES:
                logger.info(f"Fetching submissions page {page}...")
                try:
                    html = hn_fetcher.fetch_whoishiring_submissions(next_page_params)
                    consecutive_errors = 0 # Reset on success
                except RequestException:
                    consecutive_errors += 1
                    logger.error(f"Network error fetching submissions page {page}. ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS})")
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        logger.critical("Too many network errors. Stopping.")
                        sys.exit(1)
                    time.sleep(5) # Wait a bit before retry
                    continue

                if not html:
                    break
                    
                threads, next_page_params = parser.parse_thread_list(html)
                all_threads.extend(threads)
                
                if not next_page_params:
                    break
                page += 1
            
            # Save the fetched threads to cache
            if all_threads:
                checkpoint_manager.save_threads_list(all_threads)
            
        logger.info(f"Found {len(all_threads)} 'Who is hiring' threads.")
        
        # 3. Filter threads by date (2020 to 2025)
        start_date = "2020-01-01"
        
        target_threads = [t for t in all_threads if t['thread_date'] >= start_date]
        logger.info(f"Processing {len(target_threads)} threads from {start_date} to now.")
        
        for thread in target_threads:
            thread_id = thread['id']
            
            if thread_id in processed_threads:
                logger.info(f"Skipping thread {thread_id} (already processed).")
                continue

            thread_date = thread['thread_date']
            logger.info(f"Processing thread: {thread['title']} ({thread_date})")
            
            thread_comments = []
            page = 1
            consecutive_errors = 0
            MAX_CONSECUTIVE_ERRORS = config.MAX_RETRIES
            
            while True:
                try:
                    html = hn_fetcher.fetch_thread(thread_id, page=page)
                    consecutive_errors = 0
                except RequestException:
                    consecutive_errors += 1
                    logger.error(f"Network error fetching thread {thread_id} page {page}. ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS})")
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        logger.critical("Too many network errors. Saving checkpoint and stopping.")
                        checkpoint_manager.save_checkpoint(processed_threads)
                        sys.exit(1)
                    time.sleep(5)
                    continue

                if not html:
                    break
                    
                comments, has_more = parser.parse_comments(html, thread_date)
                thread_comments.extend(comments)
                
                if not has_more:
                    break
                page += 1
            
            # Save thread data immediately
            if thread_comments:
                df = pd.DataFrame(thread_comments)
                loader.save_thread_data(df, thread_id)
            
            # Mark as processed and save checkpoint
            processed_threads.add(thread_id)
            checkpoint_manager.save_checkpoint(processed_threads)
            
    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Saving checkpoint...")
        checkpoint_manager.save_checkpoint(processed_threads)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        checkpoint_manager.save_checkpoint(processed_threads)
        raise e

    # 4. Merge all data at the end
    logger.info("Merging all thread data...")
    loader.merge_thread_files()
    logger.info("Done.")

if __name__ == "__main__":
    main()
