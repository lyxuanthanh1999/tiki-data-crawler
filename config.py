import os
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "output"
INPUT_DIR = DATA_DIR / "input"
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"
FAILED_LOG_FILE = DATA_DIR / "failed_ids.txt"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
INPUT_DIR.mkdir(parents=True, exist_ok=True)

# Tiki API Configurations
TIKI_API_BASE_URL = "https://api.tiki.vn/product-detail/api/v1/products"

# Crawler & Concurrency Settings
# Gia tri khoi dau an toan; tang dan sau khi do ty le 429/HTML tren mang dang dung.
DEFAULT_CONCURRENCY = 5
DEFAULT_REQUESTS_PER_SECOND = 1.0
BATCH_SIZE = 1000  # Number of products per output .json file
MAX_RETRIES = 6    # Retry attempts on transient failure/rate limit
REQUEST_TIMEOUT = 12  # Seconds

# Request Headers to mimic legitimate browser traffic
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
    "Origin": "https://tiki.vn",
    "Referer": "https://tiki.vn/",
    "Sec-Ch-Ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}
