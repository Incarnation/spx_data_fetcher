# =====================
# workers/main.py
# Background scheduler for data fetching and analytics
# =====================
import logging
import time

from common.config import GOOGLE_CLOUD_PROJECT, GOOGLE_SERVICE_ACCOUNT_JSON, OPTION_CHAINS_TABLE_ID
from common.utils import setup_logging
from fetcher.scheduler import start_scheduler

if __name__ == "__main__":
    setup_logging()
    logging.info("📡 Background scheduler starting...")

    # Optional: double-check config presence (though common.config already does this)
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
        start_scheduler()
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("🛑 Scheduler manually stopped.")
    except Exception as e:
        logging.exception(f"💥 Unhandled exception in worker: {e}")
