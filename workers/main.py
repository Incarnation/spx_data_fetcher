# =====================
# workers/main.py
# Background scheduler for data fetching and analytics
# =====================
import logging
import os
import time

from app.scheduler import start_scheduler
from common.utils import setup_logging

if __name__ == "__main__":
    setup_logging()
    logging.info("📡 Background scheduler starting...")

    required_env_vars = [
        "GOOGLE_SERVICE_ACCOUNT_JSON",
        "GOOGLE_CLOUD_PROJECT",
        "OPTION_CHAINS_TABLE_ID",
    ]
    missing = [var for var in required_env_vars if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"❌ Missing required environment variables: {', '.join(missing)}")

    try:
        start_scheduler()
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("🛑 Scheduler manually stopped.")
    except Exception as e:
        logging.exception(f"💥 Unhandled exception in worker: {e}")
