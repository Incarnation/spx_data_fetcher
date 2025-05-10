# fetcher/scheduler.py
# =====================
# Orchestrator: market‑data + analytics + trades
#
# Cadences:
#   • upload_index_price   → every  5 min
#   • option chains + PnL   → every 10 min
#   • GEX & realized_vol    → every 15 min
#   • EOD batch (final run) → once at 16:00 ET (via same scheduled_market_data)
#   • 0DTE trade gen        → at 10:00, 11:00, 12:00, 13:00 ET
# =====================

import logging
from datetime import datetime, timezone

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from analytics.gex_calculator import calculate_and_store_gex
from analytics.realized_vol import calculate_and_store_realized_vol
from common.config import SUPPORTED_SYMBOLS  # e.g. ["SPX", "QQQ", ...]
from common.utils import is_trading_hours  # returns True 9:30–16:00 ET Mon–Fri
from fetcher.fetcher import fetch_option_chain  # fetch option chain for one expiry
from fetcher.fetcher import fetch_underlying_quote  # fetch latest index quote
from fetcher.fetcher import get_next_expirations  # list upcoming option expirations
from fetcher.uploader import upload_index_price, upload_to_bigquery
from trade.pnl_monitor import update_trade_pnl  # accepts symbol, quote, mid_maps
from trade.trade_generator import generate_0dte_trade

# ── Use a single timezone object for scheduling & conversions ───────────────
NY_TZ = pytz.timezone("America/New_York")

# ── Instantiate the background scheduler with New York timezone ─────────────
scheduler = BackgroundScheduler(timezone=NY_TZ)


def debug_heartbeat():
    """
    Heartbeat job that logs every 10 min (00,10,20,30,40,50)
    to confirm that the scheduler thread is alive.
    """
    logging.info("💓 Heartbeat: scheduler is alive.")


def scheduled_market_data():
    """
    Runs every 5 min during trading hours:
      1) ALWAYS: fetch & upload index price for each symbol
      2) ON 10‑MINUTE ticks (minute % 10 == 0):
         a) fetch & upload option chains for each expiry per symbol
         b) build per‑symbol mid_maps
         c) call update_trade_pnl(symbol, quote, mid_maps)
    At exactly 16:00 ET, is_trading_hours() still returns True (<= 16:00:59),
    so this final run also triggers the EOD PnL close inside update_trade_pnl.
    """
    # ── 0) Guard: only run during official market hours (9:30–16:00 ET) ────
    if not is_trading_hours():
        logging.debug("Market closed; skipping market_data.")
        return

    # ── 1) Capture current UTC & ET times for cadence checks & timestamps ───
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(NY_TZ)
    minute = now_et.minute
    # Determine whether this invocation is on a 10‑min boundary
    is_10min = minute % 10 == 0

    # ── 2) Loop through each supported symbol independently ────────────────
    for sym in SUPPORTED_SYMBOLS:
        # 2a) Fetch underlying index quote (one API call per symbol)
        quote = fetch_underlying_quote(sym)

        # 2b) Always upload index price (every 5 min)
        upload_index_price(sym, quote)

        # ── 3) On 10‑min ticks, also ingest options chain & update PnL ─────
        if is_10min:
            # 3a) Prepare a mid‑price map: expiry_date -> {(strike, type): mid_price}
            per_symbol_mid: dict[str, dict[tuple[float, str], float]] = {}

            # 3b) Retrieve list of upcoming expirations for this symbol
            expirations = get_next_expirations(sym)

            for exp in expirations:
                # 3c) Fetch full option chain for this expiry
                legs = fetch_option_chain(sym, exp, quote)
                if not legs:
                    # skip if API returned no data
                    continue

                # 3d) Upload raw option legs snapshot to BigQuery
                upload_to_bigquery(legs, now_utc, exp, quote)

                # 3e) Build a lookup of mid_prices for PnL computation
                per_symbol_mid[exp] = {(leg.strike, leg.option_type): leg.mid_price for leg in legs}

            # 3f) Invoke the PnL monitor once per symbol, passing mid_maps
            update_trade_pnl(symbol=sym, quote=quote, mid_maps=per_symbol_mid)


def start_scheduler():
    """
    Configure and start all scheduled jobs:
      - Heartbeat (every 10 min)
      - scheduled_market_data (every 5 min)
      - GEX & realized_vol analytics (every 15 min)
      - EOD batch at 16:00 ET (via scheduled_market_data + analytics)
      - 0DTE trade generator (at 10–13 ET sharp)
    """
    # ── If already running (hot reload), clear existing jobs first ─────────
    if scheduler.running:
        scheduler.remove_all_jobs()

    # 1) Heartbeat job – logs every 10 min, 24/7
    scheduler.add_job(
        debug_heartbeat,
        CronTrigger(minute="0,10,20,30,40,50", timezone=NY_TZ),
    )

    # 2) Market‑data job: every 5 min Mon–Fri, 9:00–15:55 ET
    scheduler.add_job(
        scheduled_market_data,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-15",
            minute="0,5,10,15,20,25,30,35,40,45,50,55",
            timezone=NY_TZ,
        ),
    )

    # 3) GEX analytics: every 15 min Mon–Fri, 9:00–15:45 ET
    scheduler.add_job(
        calculate_and_store_gex,
        CronTrigger(day_of_week="mon-fri", hour="9-15", minute="0,15,30,45", timezone=NY_TZ),
    )

    # 4) Realized‑vol analytics: every 15 min Mon–Fri, 9:00–15:45 ET
    scheduler.add_job(
        calculate_and_store_realized_vol,
        CronTrigger(day_of_week="mon-fri", hour="9-15", minute="0,15,30,45", timezone=NY_TZ),
    )

    # 5) End‑of‑day snapshot at exactly 16:00 ET:
    #    scheduled_market_data will run (and because is_trading_hours() is True),
    #    its 10‑min branch fires (minute==0), and update_trade_pnl() will detect
    #    EOD inside and close out legs.
    eod_trigger = CronTrigger(day_of_week="mon-fri", hour="16", minute="0", timezone=NY_TZ)
    scheduler.add_job(scheduled_market_data, eod_trigger)
    scheduler.add_job(calculate_and_store_gex, eod_trigger)
    scheduler.add_job(calculate_and_store_realized_vol, eod_trigger)

    # 6) 0DTE Iron‑Condor trade generator at 10:00,11:00,12:00,13:00 ET sharp
    scheduler.add_job(
        generate_0dte_trade,
        CronTrigger(day_of_week="mon-fri", hour="10,11,12,13", minute="0", timezone=NY_TZ),
    )

    # ── Start the APScheduler background thread ─────────────────────────────
    scheduler.start()
    logging.info("📅 Scheduler running: 5/10/15 min cadences + EOD + 0DTE jobs.")


def shutdown_scheduler():
    """
    Gracefully stop the scheduler thread if it’s currently running.
    """
    if scheduler.running:
        scheduler.shutdown()
        logging.info("🛑 Scheduler stopped.")
