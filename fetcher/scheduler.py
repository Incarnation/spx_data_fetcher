# =====================
# fetcher/scheduler.py
# Runs fetch every 10 minutes during trading hours for multiple symbols
# Also schedules analytics jobs
# =====================

import logging
from datetime import datetime, timezone

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from analytics.gex_calculator import calculate_and_store_gex
from analytics.realized_vol import calculate_and_store_realized_vol
from common.config import SUPPORTED_SYMBOLS
from common.utils import is_trading_hours
from fetcher.fetcher import fetch_option_chain, fetch_underlying_quote, get_next_expirations
from fetcher.uploader import upload_index_price, upload_to_bigquery
from trade.trade_generator import generate_0dte_trade

# Set the timezone to Eastern Time (EST/EDT)
NY_TZ = pytz.timezone("America/New_York")
scheduler = BackgroundScheduler(timezone=NY_TZ)


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

    # Heartbeat every 10 minutes (24/7)
    scheduler.add_job(
        debug_heartbeat,
        CronTrigger(minute="0,10,20,30,40,50", timezone=NY_TZ),
    )

    # Index price upload every 5 minutes from 9:30 AM to 4:00 PM EST
    scheduler.add_job(
        scheduled_upload_index_price,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-16",
            minute="0,5,10,15,20,25,30,35,40,45,50,55",
            timezone=NY_TZ,
        ),
    )

    # Options data fetch every 10 minutes from 9:30 AM to 4:00 PM EST
    scheduler.add_job(
        scheduled_fetch_and_upload_options_data,
        CronTrigger(day_of_week="mon-fri", hour="9-16", minute="0,10,20,30,40,50", timezone=NY_TZ),
    )

    # GEX calculation every 15 minutes from 9:30 AM to 4:00 PM EST
    scheduler.add_job(
        calculate_and_store_gex,
        CronTrigger(day_of_week="mon-fri", hour="9-16", minute="0,15,30,45", timezone=NY_TZ),
    )

    # Realized volatility every 5 minutes from 9:30 AM to 4:00 PM EST
    scheduler.add_job(
        calculate_and_store_realized_vol,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-16",
            minute="0,5,10,15,20,25,30,35,40,45,50,55",
            timezone=NY_TZ,
        ),
    )

    # Schedule 0DTE Iron Condor generation at 10AM, 11AM, 12PM, 1PM
    scheduler.add_job(
        generate_0dte_trade, "cron", day_of_week="mon-fri", hour="10,11,12,13", minute="0"
    )

    scheduler.start()
    logging.info("📅 Scheduler started for 9:30 AM to 4:00 PM EST.")


def shutdown_scheduler():
    """Shutdown the scheduler if running."""
    if scheduler.running:
        scheduler.shutdown()
        logging.info("🛑 Scheduler shut down.")
