# fetcher/scheduler.py
# =====================
# Orchestrator: market‑data + analytics + trades
#
# Cadences:
#   • upload_index_price   → every  5 min
#   • option chains + PnL   → every 10 min
#   • GEX & realized_vol    → every 15 min
#   • EOD batch (final run) → once at 16:00 ET
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
from common.utils import is_trading_hours  # returns True during 9:30–16:00 ET
from fetcher.fetcher import fetch_option_chain  # retrieves options chain for an expiry
from fetcher.fetcher import fetch_underlying_quote  # retrieves latest index quote
from fetcher.fetcher import get_next_expirations  # lists upcoming expiries for a symbol
from fetcher.uploader import upload_index_price, upload_to_bigquery
from trade.pnl_monitor import update_trade_pnl  # now accepts symbol + mid_maps
from trade.trade_generator import generate_0dte_trade

# ── Use a single timezone constant for all scheduling & conversions ───────────
NY_TZ = pytz.timezone("America/New_York")

# ── Create a background scheduler with New York timezone ───────────────────────
scheduler = BackgroundScheduler(timezone=NY_TZ)


def debug_heartbeat():
    """
    Simple heartbeat job that logs every 10 min to confirm the scheduler is alive.
    Runs 24/7 (outside market hours too).
    """
    logging.info("💓 Heartbeat: scheduler is alive.")


def scheduled_market_data():
    """
    Main market‐data job that runs every 5 min during trading hours.
    - Always (every 5 min): fetch & upload index price for each symbol.
    - Additionally (every 10 min): fetch & upload option chains, build mid_maps,
      then update trade PnL for each symbol.
    """
    # Skip if outside 9:30–16:00 ET (pre/post-market)
    if not is_trading_hours():
        logging.debug("Market closed; skipping market-data tasks.")
        return

    # Current times in UTC & ET, used for timestamps and cadence checks
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(NY_TZ)
    minute = now_et.minute
    is_10min = minute % 10 == 0

    # Loop over each supported symbol individually
    for sym in SUPPORTED_SYMBOLS:
        # 1️⃣ Fetch the underlying index quote once per symbol per run
        quote = fetch_underlying_quote(sym)
        # 2️⃣ Always upload index price snapshot (every 5 min)
        upload_index_price(sym, quote)

        # 3️⃣ On 10‑min ticks, also ingest option chains & update PnL
        if is_10min:
            # Build a per-symbol mapping: expiry_date -> {(strike, type): mid_price}
            per_symbol_mid: dict[str, dict[tuple[float, str], float]] = {}

            # Get the next expirations to fetch
            expirations = get_next_expirations(sym)

            for exp_date in expirations:
                # Fetch options for this expiry using the quote we already have
                option_legs = fetch_option_chain(sym, exp_date, quote)
                if not option_legs:
                    # If no data returned, skip this expiry
                    continue

                # Upload raw option chain snapshot to BigQuery
                upload_to_bigquery(option_legs, now_utc, exp_date, quote)

                # Build mid‑price map for this expiry
                per_symbol_mid[exp_date] = {
                    (leg.strike, leg.option_type): leg.mid_price for leg in option_legs
                }

            # 4️⃣ After fetching all expiries for this symbol, update PnL
            #     Pass the symbol, its quote, and the mid_maps built above
            update_trade_pnl(symbol=sym, quote=quote, mid_maps=per_symbol_mid)


def start_scheduler():
    """
    Wire up all scheduled jobs and start the background scheduler.
    """
    # Remove existing jobs on restart to avoid duplicates
    if scheduler.running:
        scheduler.remove_all_jobs()

    # ─── 1) Heartbeat: every 10 min, 24/7 ────────────────────────────────
    scheduler.add_job(
        debug_heartbeat,
        CronTrigger(minute="0,10,20,30,40,50", timezone=NY_TZ),
    )

    # ─── 2) Market‑data job (index + options + PnL):
    #       every 5 min Mon–Fri, 9:00–15:55 ET
    scheduler.add_job(
        scheduled_market_data,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-15",
            minute="0,5,10,15,20,25,30,35,40,45,50,55",
            timezone=NY_TZ,
        ),
    )

    # ─── 3) GEX analytics: every 15 min Mon–Fri, 9:00–15:45 ET ────────────
    scheduler.add_job(
        calculate_and_store_gex,
        CronTrigger(day_of_week="mon-fri", hour="9-15", minute="0,15,30,45", timezone=NY_TZ),
    )

    # ─── 4) Realized Vol analytics: every 15 min Mon–Fri, 9:00–15:45 ET ─────
    scheduler.add_job(
        calculate_and_store_realized_vol,
        CronTrigger(day_of_week="mon-fri", hour="9-15", minute="0,15,30,45", timezone=NY_TZ),
    )

    # ─── 5) End‑of‑day batch at 16:00 ET: final snapshots & analytics ─────
    eod_trigger = CronTrigger(day_of_week="mon-fri", hour="16", minute="0", timezone=NY_TZ)
    for fn in (
        scheduled_market_data,  # one more market-data run
        calculate_and_store_gex,  # final GEX snapshot
        calculate_and_store_realized_vol,  # final realized-vol snapshot
        # final PnL close for all symbols
        lambda: [update_trade_pnl(symbol=s, quote=None, mid_maps=None) for s in SUPPORTED_SYMBOLS],
    ):
        scheduler.add_job(fn, eod_trigger)

    # ─── 6) 0DTE Iron Condor generator:
    #       at 10:00, 11:00, 12:00, 13:00 ET on trading days
    scheduler.add_job(
        generate_0dte_trade,
        CronTrigger(day_of_week="mon-fri", hour="10,11,12,13", minute="0", timezone=NY_TZ),
    )

    # Start the scheduler thread
    scheduler.start()
    logging.info("📅 Scheduler running: 5/10/15 min cadences + EOD + 0DTE jobs.")


def shutdown_scheduler():
    """
    Gracefully shut down the scheduler if running.
    """
    if scheduler.running:
        scheduler.shutdown()
        logging.info("🛑 Scheduler stopped.")
