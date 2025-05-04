# =====================
# app/scheduler.py
# Runs every 10 minutes during market hours, fetches and uploads data
# =====================
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import logging
from .fetcher import fetch_option_chain, get_next_expirations, fetch_underlying_price
from .uploader import upload_to_bigquery
from .utils import is_market_open


def scheduled_job():
    now = datetime.utcnow()
    if not is_market_open(now):
        logging.info(f"[{now}] Market closed — skipping fetch.")
        return

    underlying_price = fetch_underlying_price()
    expirations = get_next_expirations()
    for expiry in expirations:
        logging.info(f"[{now}] Fetching options for {expiry}")
        data = fetch_option_chain(expiration=expiry)
        if data:
            upload_to_bigquery(data, now, expiry, underlying_price)
            logging.info(f"Uploaded {len(data)} rows for {expiry}")
        else:
            logging.warning(f"No data for {expiry}")


def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_job, "interval", minutes=15)
    scheduler.start()
