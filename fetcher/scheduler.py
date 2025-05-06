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


def scheduled_fetch():
    if not is_trading_hours():
        logging.info("⏳ Market closed, skipping fetch.")
        return

    now = datetime.now(timezone.utc)

    for symbol in SUPPORTED_SYMBOLS:
        expirations = get_next_expirations(symbol)
        underlying_quote = fetch_underlying_quote(symbol)
        upload_index_price(symbol, underlying_quote)

        for expiry in expirations:
            options = fetch_option_chain(symbol, expiry, underlying_quote)
            if options:
                upload_to_bigquery(options, now, expiry, underlying_quote)
                logging.info(f"✅ {symbol} {expiry} - {len(options)} options uploaded.")
            else:
                logging.warning(f"⚠️ No options fetched for {symbol} {expiry}")


def start_scheduler():
    scheduler.add_job(debug_heartbeat, "interval", minutes=5)
    scheduler.add_job(scheduled_fetch, "interval", minutes=10)
    scheduler.add_job(calculate_and_store_gex, "interval", minutes=15)
    scheduler.add_job(calculate_and_store_realized_vol, "interval", minutes=15)
    scheduler.start()
    logging.info("📅 Scheduler started: options data fetch every 10m, analytics every 15m")


def shutdown_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        logging.info("🛑 Scheduler shut down")
