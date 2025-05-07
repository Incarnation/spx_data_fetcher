# =====================
# common/config.py
# Centralized configuration for environment variables
# =====================

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env for local development
if not (os.getenv("RENDER") or os.getenv("RAILWAY_ENVIRONMENT")):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".." / ".env")

# Environment variables
TRADIER_API_KEY = os.getenv("TRADIER_API_KEY")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
OPTION_CHAINS_TABLE_ID = os.getenv("OPTION_CHAINS_TABLE_ID")
INDEX_PRICE_TABLE_ID = os.getenv("INDEX_PRICE_TABLE_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BASE_URL = "https://api.tradier.com/v1/markets"
CONTRACT_MULTIPLIER = 100
FETCH_INTERVAL_MIN = 10
GEX_INTERVAL_MIN = 15
SUPPORTED_SYMBOLS = ["SPX"]
SOURCE = "tradier"
INDEX_PRICE_TIME_INTERVAL = "5m"

# Sanity checks (optional but helpful for debugging)
REQUIRED_VARS = {
    "TRADIER_API_KEY": TRADIER_API_KEY,
    "GOOGLE_CLOUD_PROJECT": GOOGLE_CLOUD_PROJECT,
    "OPTION_CHAINS_TABLE_ID": OPTION_CHAINS_TABLE_ID,
    "INDEX_PRICE_TABLE_ID": INDEX_PRICE_TABLE_ID,
}

missing = [key for key, value in REQUIRED_VARS.items() if not value]
if missing and not os.getenv("RAILWAY_ENVIRONMENT"):
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")
