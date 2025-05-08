# fetcher/scheduler.py
# =====================
# Orchestrator: fetch market data, run analytics, generate trades, monitor PnL
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
from trade.pnl_monitor import update_trade_pnl
from trade.trade_generator import generate_0dte_trade

# Use Eastern Time for all market‑hours scheduling
NY_TZ = pytz.timezone("America/New_York")
scheduler = BackgroundScheduler(timezone=NY_TZ)


def debug_heartbeat():
    logging.info("💓 Heartbeat alive.")


def scheduled_upload_index_price():
    """Every 5m in market hours, fetch & upload SPX index price."""
    if not is_trading_hours():
        return
    for sym in SUPPORTED_SYMBOLS:
        q = fetch_underlying_quote(sym)
        upload_index_price(sym, q)


def scheduled_fetch_and_upload_options_data():
    """Every 10m in market hours, fetch & upload SPX option chains."""
    if not is_trading_hours():
        return
    now = datetime.now(timezone.utc)
    for sym in SUPPORTED_SYMBOLS:
        quote = fetch_underlying_quote(sym)
        exps = get_next_expirations(sym)
        for exp in exps:
            opts = fetch_option_chain(sym, exp, quote)
            if opts:
                upload_to_bigquery(opts, now, exp, quote)


def start_scheduler():
    """Wire up all jobs and start the background scheduler."""
    if scheduler.running:
        scheduler.remove_all_jobs()

    # 1) Heartbeat (every 10m, all day)
    scheduler.add_job(debug_heartbeat, CronTrigger(minute="0,10,20,30,40,50", timezone=NY_TZ))

    # 2) Market‑hours tasks: 9:30–15:55 ET
    market_cron = dict(day_of_week="mon-fri", hour="9-15")
    scheduler.add_job(
        scheduled_upload_index_price,
        CronTrigger(**market_cron, minute="0,5,10,15,20,25,30,35,40,45,50,55", timezone=NY_TZ),
    )
    scheduler.add_job(
        scheduled_fetch_and_upload_options_data,
        CronTrigger(**market_cron, minute="0,10,20,30,40,50", timezone=NY_TZ),
    )
    scheduler.add_job(
        calculate_and_store_gex, CronTrigger(**market_cron, minute="0,15,30,45", timezone=NY_TZ)
    )
    scheduler.add_job(
        calculate_and_store_realized_vol,
        CronTrigger(**market_cron, minute="0,5,10,15,20,25,30,35,40,45,50,55", timezone=NY_TZ),
    )
    scheduler.add_job(update_trade_pnl, CronTrigger(**market_cron, minute="*/5", timezone=NY_TZ))

    # 3) Final EOD run at 16:00 ET
    eod_trigger = CronTrigger(day_of_week="mon-fri", hour="16", minute="0", timezone=NY_TZ)
    for fn in (
        scheduled_upload_index_price,
        scheduled_fetch_and_upload_options_data,
        calculate_and_store_gex,
        calculate_and_store_realized_vol,
        update_trade_pnl,
    ):
        scheduler.add_job(fn, eod_trigger)

    # 4) 0DTE Iron Condor generator at 10,11,12,13 sharp
    scheduler.add_job(
        generate_0dte_trade,
        CronTrigger(day_of_week="mon-fri", hour="10,11,12,13", minute="0", timezone=NY_TZ),
    )

    scheduler.start()
    logging.info("📅 Scheduler running: market hours + trade/PnL jobs.")


def shutdown_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        logging.info("🛑 Scheduler stopped.")
