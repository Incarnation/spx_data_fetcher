# =====================
# fetcher/scheduler.py
# Runs fetch every 10 minutes during trading hours for multiple symbols
# Also schedules analytics jobs
# =====================

import logging
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler

from analytics.gex_calculator import calculate_and_store_gex
from analytics.realized_vol import calculate_and_store_realized_vol
from common.config import SUPPORTED_SYMBOLS
from common.utils import is_trading_hours
from fetcher.fetcher import fetch_option_chain, fetch_underlying_quote, get_next_expirations
from fetcher.uploader import upload_index_price, upload_to_bigquery

scheduler = BackgroundScheduler()


def debug_heartbeat():
    logging.info("💓 Heartbeat: scheduler is alive.")


def scheduled_upload_index_price():
    """Fetch and upload index price for each supported symbol."""
    if not is_trading_hours():
        logging.info("⏳ Market closed, skipping index price upload.")
        return

    for symbol in SUPPORTED_SYMBOLS:
        try:
            quote = fetch_underlying_quote(symbol)
            upload_index_price(symbol, quote)
            logging.info(f"✅ Uploaded index price for {symbol}")
        except Exception as e:
            logging.exception(f"💥 Failed to upload index price for {symbol}: {e}")


def scheduled_fetch_and_upload_options_data():
    """Fetch option chains and upload to BigQuery."""
    if not is_trading_hours():
        logging.info("⏳ Market closed, skipping option chain fetch.")
        return

    now = datetime.now(timezone.utc)

    for symbol in SUPPORTED_SYMBOLS:
        try:
            quote = fetch_underlying_quote(symbol)
            expirations = get_next_expirations(symbol)

            for expiry in expirations:
                options = fetch_option_chain(symbol, expiry, quote)
                if options:
                    upload_to_bigquery(options, now, expiry, quote)
                    logging.info(f"✅ {symbol} {expiry} - {len(options)} options uploaded.")
                else:
                    logging.warning(f"⚠️ No options fetched for {symbol} {expiry}")
        except Exception as e:
            logging.exception(f"💥 Error processing {symbol}: {e}")


def start_scheduler():
    """Initialize and start the background scheduler."""
    if scheduler.running:
        scheduler.remove_all_jobs()
        logging.info("♻️ Cleaned up existing jobs.")

    scheduler.add_job(debug_heartbeat, "interval", minutes=5)
    scheduler.add_job(scheduled_upload_index_price, "interval", minutes=5)
    scheduler.add_job(scheduled_fetch_and_upload_options_data, "interval", minutes=10)
    scheduler.add_job(calculate_and_store_gex, "interval", minutes=15)
    scheduler.add_job(calculate_and_store_realized_vol, "interval", minutes=5)

    scheduler.start()
    logging.info("📅 Scheduler started: price every 5m, options every 10m, analytics every 15m")


def shutdown_scheduler():
    """Shutdown the scheduler if running."""
    if scheduler.running:
        scheduler.shutdown()
        logging.info("🛑 Scheduler shut down.")
