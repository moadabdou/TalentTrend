import time
import logging
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from . import config

logger = logging.getLogger(__name__)

class HNFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': config.USER_AGENT})
        retries = Retry(
            total=config.MAX_RETRIES,
            backoff_factor=config.BACKOFF_FACTOR,
            status_forcelist=[403, 429, 500, 502, 503, 504],
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.last_request_time = 0

    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        # Add random jitter (0.5 to 1.5 seconds) to the base delay
        delay = config.RATE_LIMIT_DELAY + random.uniform(0.5, 1.5)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self.last_request_time = time.time()

    def fetch_url(self, url):
        self._rate_limit()
        try:
            logger.info(f"Fetching {url}")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            # Re-raise exception to let the caller handle the stop condition
            raise e


    def fetch_whoishiring_submissions(self, next_page_params=None):
        if next_page_params:
            # Ensure we don't double slash if base url ends with /
            base = config.BASE_URL.rstrip('/')
            params = next_page_params.lstrip('/')
            url = f"{base}/{params}"
        else:
            url = config.USER_SUBMISSIONS_URL
        return self.fetch_url(url)

    def fetch_thread(self, thread_id, page=1):
        url = f"{config.BASE_URL}/item?id={thread_id}&p={page}"
        return self.fetch_url(url)
