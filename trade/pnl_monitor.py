# =====================
# trade/pnl_monitor.py
# =====================
# PnL Monitoring, live_trade_pnl snapshots + trade‑level closure
# =====================

import logging
from datetime import datetime, timezone

from google.cloud import bigquery

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT, INDEX_PRICE_TABLE_ID

# Table definitions
TRADE_RECS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
TRADE_LEGS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
OPTION_SNAP_TABLE = f"{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot"
LIVE_TRADE_PNL_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.live_trade_pnl"

# One‐time BigQuery client
CREDS = get_gcp_credentials()
CLIENT = bigquery.Client(credentials=CREDS, project=GOOGLE_CLOUD_PROJECT)


def update_trade_pnl():
    """
    1) Snapshot each open leg’s current mid & PnL into live_trade_pnl.
    2) Update trade_legs rows with latest pnl, status (and exit_price at EOD).
    3) Aggregate to trade_recommendations: pnl, status, exit_time, exit_price (at EOD).
    """
    logging.info("🔄 Running PnL monitor…")

    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.isoformat()
    # 4 PM ET == 21:00 UTC
    is_eod = now_utc.hour == 21 and now_utc.minute == 0

    # 1️⃣ Fetch all open legs
    legs_q = f"""
      SELECT trade_id, leg_id, strike, direction, entry_price
      FROM `{TRADE_LEGS_TABLE}`
      WHERE status = 'open'
    """
    legs_df = CLIENT.query(legs_q).to_dataframe()
    if legs_df.empty:
        logging.info("No open legs to update.")
        return

    # 2️⃣ Fetch latest mid-prices for SPXW
    snap_q = f"""
      SELECT strike, mid_price
      FROM `{OPTION_SNAP_TABLE}`
      WHERE timestamp = (
        SELECT MAX(timestamp)
        FROM `{OPTION_SNAP_TABLE}`
      )
      AND symbol = 'SPXW'
    """
    mids_df = CLIENT.query(snap_q).to_dataframe()
    mid_map = mids_df.set_index("strike")["mid_price"].to_dict()

    # 3️⃣ Fetch current SPX underlying
    spot_q = f"""
      SELECT last AS underlying_price
      FROM `{INDEX_PRICE_TABLE_ID}`
      WHERE symbol = 'SPX'
      ORDER BY timestamp DESC
      LIMIT 1
    """
    spot_df = CLIENT.query(spot_q).to_dataframe()
    underlying_price = spot_df["underlying_price"].iloc[0] if not spot_df.empty else None

    live_rows = []
    leg_updates = []
    trade_totals = {}

    # 4️⃣ For each open leg, compute PnL and prepare snapshots
    for _, leg in legs_df.iterrows():
        trade_id = leg["trade_id"]
        leg_id = leg["leg_id"]
        strike = leg["strike"]
        direction = leg["direction"]
        entry = leg["entry_price"]
        current = mid_map.get(strike, entry)

        # PnL: shorts gain when price falls; longs vice versa
        pnl = (entry - current) if direction == "short" else (current - entry)
        status = "closed" if is_eod else "open"
        exit_price_leg = current if is_eod else None

        # --- snapshot for live_trade_pnl
        live_rows.append(
            {
                "trade_id": trade_id,
                "leg_id": leg_id,
                "timestamp": now_iso,
                "current_price": current,
                "theoretical_pnl": pnl,
                "mark_price": current,
                "underlying_price": underlying_price,
                "price_type": "mid",
                "underlying_symbol": "SPX",
                "status": status,
            }
        )

        # --- prepare leg row update
        leg_updates.append(
            {
                "leg_id": leg_id,
                "pnl": pnl,
                "status": status,
                "exit_price": exit_price_leg,
            }
        )

        # --- accumulate trade PnL
        trade_totals[trade_id] = trade_totals.get(trade_id, 0) + pnl

    # 1️⃣ Bulk insert live snapshots
    errors = CLIENT.insert_rows_json(LIVE_TRADE_PNL_TABLE, live_rows)
    if errors:
        logging.error("❌ live_trade_pnl insert errors: %s", errors)

    # 2️⃣ UPDATE each leg row
    for upd in leg_updates:
        set_parts = [f"pnl = {upd['pnl']}", f"status = '{upd['status']}'"]
        if is_eod:
            set_parts.append(f"exit_price = {upd['exit_price']}")
        set_sql = ", ".join(set_parts)

        CLIENT.query(
            f"""
          UPDATE `{TRADE_LEGS_TABLE}`
          SET {set_sql}
          WHERE leg_id = '{upd['leg_id']}'
        """
        )

    # 3️⃣ UPDATE each trade record
    for trade_id, total_pnl in trade_totals.items():
        status_sql = "'closed'" if is_eod else "'active'"
        exit_time_sql = "CURRENT_TIMESTAMP()" if is_eod else "exit_time"
        exit_price_sql = f"entry_price + {total_pnl}" if is_eod else "exit_price"

        CLIENT.query(
            f"""
          UPDATE `{TRADE_RECS_TABLE}`
          SET
            pnl        = {total_pnl},
            status     = {status_sql},
            exit_time  = {exit_time_sql}
            {', exit_price = ' + exit_price_sql if is_eod else ''}
          WHERE trade_id = '{trade_id}'
        """
        )

    logging.info("✅ PnL monitor complete (EOD=%s)", is_eod)
