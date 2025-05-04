# =====================
# workers/main.py
# Background scheduler for data fetching and analytics
# =====================
import logging
import sys
import time
from pathlib import Path

# Ensure the parent project root is on the Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.scheduler import start_scheduler
from app.utils import setup_logging

if __name__ == "__main__":
    setup_logging()
    logging.info("📡 Background scheduler running...")
    start_scheduler()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("🛑 Scheduler manually stopped.")
