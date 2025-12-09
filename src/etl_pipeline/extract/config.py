import os

BASE_URL = "https://news.ycombinator.com"
USER_SUBMISSIONS_URL = "https://news.ycombinator.com/submitted?id=whoishiring"
RATE_LIMIT_DELAY = 3.0  # seconds
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")
CHECKPOINT_FILE = os.path.join(DATA_DIR, "checkpoint.json")
THREADS_LIST_FILE = os.path.join(DATA_DIR, "threads_list.json")
THREADS_DIR = os.path.join(DATA_DIR, "threads")

# Ensure data directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(THREADS_DIR, exist_ok=True)
