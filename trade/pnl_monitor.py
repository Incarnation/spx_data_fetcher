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
TRADE_RECOMMENDATIONS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
TRADE_LEGS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
OPTION_SNAPSHOT_TABLE = f"{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot"
LIVE_TRADE_PNL_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.live_trade_pnl"

CRED = get_gcp_credentials()
CLIENT = bigquery.Client(credentials=CRED, project=GOOGLE_CLOUD_PROJECT)


def update_trade_pnl():
    """
    1) Snapshot each open leg’s current mid & PnL into live_trade_pnl.
    2) Update trade_legs rows with latest pnl, status/exit_price at EOD.
    3) Aggregate to trade_recommendations: pnl, status, exit_time, exit_price.
    """
    logging.info("🔄 Running PnL monitor…")

    now_utc = datetime.now(timezone.utc)
    # 4 PM ET == 21:00 UTC
    is_eod = now_utc.hour == 21 and now_utc.minute == 0

    # 1) load open legs
    q_legs = f"""
      SELECT trade_id, leg_id, strike, direction, entry_price
      FROM `{TRADE_LEGS_TABLE}`
      WHERE status='open'
    """
    legs_df = CLIENT.query(q_legs).to_dataframe()
    if legs_df.empty:
        logging.info("No open legs found.")
        return

    # 2) load latest mid prices from snapshot
    q_snap = f"""
      SELECT strike, mid_price
      FROM `{OPTION_SNAPSHOT_TABLE}`
      WHERE timestamp=(SELECT MAX(timestamp) FROM `{OPTION_SNAPSHOT_TABLE}`)
    """
    mids = CLIENT.query(q_snap).to_dataframe()
    mid_map = mids.set_index("strike")["mid_price"].to_dict()

    # 3) get current underlying
    q_spot = f"""
      SELECT last AS underlying_price
      FROM `{INDEX_PRICE_TABLE_ID}`
      WHERE symbol='SPX'
      ORDER BY timestamp DESC
      LIMIT 1
    """
    sp = CLIENT.query(q_spot).to_dataframe()
    underlying_price = sp["underlying_price"].iloc[0] if not sp.empty else None

    live_rows = []
    leg_updates = []
    trade_totals = {}

    for _, leg in legs_df.iterrows():
        trade_id, leg_id = leg["trade_id"], leg["leg_id"]
        strike, direction = leg["strike"], leg["direction"]
        entry = leg["entry_price"]
        current = mid_map.get(strike, entry)

        # PnL: shorts gain when price falls, longs gain when price rises
        pnl = (entry - current) if direction == "short" else (current - entry)
        status = "closed" if is_eod else "open"
        exit_price_leg = current if is_eod else None

        # 1️⃣ snapshot row
        live_rows.append(
            {
                "trade_id": trade_id,
                "leg_id": leg_id,
                "timestamp": now_utc,
                "current_price": current,
                "theoretical_pnl": pnl,
                "mark_price": current,
                "underlying_price": underlying_price,
                "price_type": "mid",
                "underlying_symbol": "SPX",
                "status": status,
            }
        )

        # 2️⃣ collect leg update
        leg_updates.append(
            {"leg_id": leg_id, "pnl": pnl, "status": status, "exit_price": exit_price_leg}
        )

        # 3️⃣ aggregate per trade
        trade_totals[trade_id] = trade_totals.get(trade_id, 0) + pnl

    # Bulk insert live snapshots
    CLIENT.insert_rows_json(LIVE_TRADE_PNL_TABLE, live_rows)

    # Update each leg
    for u in leg_updates:
        set_clauses = [f"pnl = {u['pnl']}", f"status = '{u['status']}'"]
        if is_eod:
            set_clauses.append(f"exit_price = {u['exit_price']}")
        set_sql = ", ".join(set_clauses)
        CLIENT.query(
            f"""
          UPDATE `{TRADE_LEGS_TABLE}`
          SET {set_sql}
          WHERE leg_id = '{u['leg_id']}'
        """
        )

    # Update each trade record
    for tid, total in trade_totals.items():
        status = "closed" if is_eod else "active"
        exit_time_sql = "CURRENT_TIMESTAMP()" if is_eod else "exit_time"
        exit_price_sql = f"entry_price + {total}" if is_eod else "exit_price"
        CLIENT.query(
            f"""
          UPDATE `{TRADE_RECOMMENDATIONS_TABLE}`
          SET pnl = {total},
              status = '{status}',
              exit_time = {exit_time_sql}
              {', exit_price = ' + exit_price_sql if is_eod else ''}
          WHERE trade_id = '{tid}'
        """
        )

    logging.info("✅ PnL monitor complete (EOD=%s)", is_eod)
