# =====================
# workers/main.py
# Background scheduler for data fetching and analytics
# =====================
import logging
import time

from app.scheduler import start_scheduler
from common.utils import setup_logging

if __name__ == "__main__":
    setup_logging()
    logging.info("📡 Background scheduler running...")
    start_scheduler()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("🛑 Scheduler manually stopped.")
    except Exception as e:
        logging.exception(f"💥 Unhandled exception in worker: {e}")
