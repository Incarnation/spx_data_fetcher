# =====================
# workers/main.py
# Background scheduler for data fetching and analytics
# =====================

import os
import sys

# Adjust the Python path to include the project root
if not (os.getenv("RENDER") or os.getenv("RAILWAY_ENVIRONMENT")):
    from pathlib import Path

    project_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(project_root))  # Insert at the beginning to ensure precedence

import logging
import time

from common.config import GOOGLE_CLOUD_PROJECT, GOOGLE_SERVICE_ACCOUNT_JSON, OPTION_CHAINS_TABLE_ID
from common.utils import setup_logging
from fetcher.scheduler import start_scheduler

if __name__ == "__main__":
    setup_logging()
    logging.info("📡 Background scheduler starting...")

    # Configuration Check
    missing = []
    if not GOOGLE_CLOUD_PROJECT:
        missing.append("GOOGLE_CLOUD_PROJECT")
    if not OPTION_CHAINS_TABLE_ID:
        missing.append("OPTION_CHAINS_TABLE_ID")
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON")

    if missing:
        raise EnvironmentError(f"❌ Missing required configuration: {', '.join(missing)}")

    try:
        # Start the scheduler
        start_scheduler()
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("🛑 Scheduler manually stopped.")
    except Exception as e:
        logging.exception(f"💥 Unhandled exception in worker: {e}")
