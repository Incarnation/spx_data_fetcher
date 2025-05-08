# trade/pnl_monitor.py
# =====================
# PnL Monitoring, live_trade_pnl snapshots + trade‑level closure
# =====================
import logging
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT, INDEX_PRICE_TABLE_ID

# Fully‑qualified table names
TRADE_RECS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
TRADE_LEGS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
OPTION_SNAP_TABLE = f"{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot"
LIVE_PNL_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.live_trade_pnl"

# Instantiate BigQuery client once
CLIENT = bigquery.Client(
    credentials=get_gcp_credentials(),
    project=GOOGLE_CLOUD_PROJECT,
)


def update_trade_pnl():
    """
    1) Snapshot each open leg's current mid & PnL into live_trade_pnl.
    2) Update trade_legs with latest pnl, status, exit_price at EOD.
    3) Aggregate per-trade and write pnl/status/exit_time/exit_price to trade_recommendations.
    """
    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.isoformat()  # ← JSON API requires string
    is_eod = now_utc.hour == 21 and now_utc.minute == 0  # 4 PM ET = 21:00 UTC

    # ── 1️⃣ Pull open legs ─────────────────────────────────────────────────────────
    legs_df = CLIENT.query(
        f"""
      SELECT trade_id, leg_id, strike, leg_type, direction, entry_price
      FROM `{TRADE_LEGS_TABLE}`
      WHERE status='open'
    """
    ).to_dataframe()
    if legs_df.empty:
        logging.info("No open legs to process.")
        return

    # ── 2️⃣ Fetch latest mid prices for only those strikes/types ────────────────────
    conds = " OR ".join(
        f"(strike={r.strike} AND option_type='{r.leg_type}')"
        for _, r in legs_df[["strike", "leg_type"]].drop_duplicates().iterrows()
    )
    mids_df = CLIENT.query(
        f"""
      SELECT strike, option_type, mid_price
      FROM `{OPTION_SNAP_TABLE}`
      WHERE timestamp = (SELECT MAX(timestamp) FROM `{OPTION_SNAP_TABLE}`)
        AND root_symbol='SPXW'
        AND ({conds})
    """
    ).to_dataframe()
    mid_map = {(r.strike, r.option_type): r.mid_price for _, r in mids_df.iterrows()}

    # ── 3️⃣ Fetch current SPX underlying ───────────────────────────────────────────
    spot_df = CLIENT.query(
        f"""
      SELECT last AS underlying_price
      FROM `{INDEX_PRICE_TABLE_ID}`
      WHERE symbol='SPX'
      ORDER BY timestamp DESC
      LIMIT 1
    """
    ).to_dataframe()
    underlying = spot_df["underlying_price"].iloc[0] if not spot_df.empty else None

    live_rows, leg_updates, trade_totals = [], [], {}

    # ── 4️⃣ For each leg, compute PnL & prepare snapshot
    for _, leg in legs_df.iterrows():
        key = (leg.strike, leg.leg_type)
        current = mid_map.get(key, leg.entry_price)
        # short P/L = entry - current; long P/L = current - entry
        pnl = float(
            (leg.entry_price - current) if leg.direction == "short" else (current - leg.entry_price)
        )
        status = "closed" if is_eod else "open"
        exit_price = float(current) if is_eod else None

        live_rows.append(
            {
                "trade_id": leg.trade_id,
                "leg_id": leg.leg_id,
                "timestamp": now_iso,
                "current_price": float(current),
                "theoretical_pnl": pnl,
                "mark_price": float(current),
                "underlying_price": underlying,
                "price_type": "mid",
                "underlying_symbol": "SPX",
                "status": status,
            }
        )
        leg_updates.append((leg.leg_id, pnl, status, exit_price))
        trade_totals[leg.trade_id] = trade_totals.get(leg.trade_id, 0.0) + pnl

    # ── 5️⃣ Insert live snapshots ───────────────────────────────────────────────────
    errs = CLIENT.insert_rows_json(LIVE_PNL_TABLE, live_rows)
    if errs:
        logging.error("❌ live_trade_pnl insert errors: %s", errs)

    # ── 6️⃣ Update each leg row with new pnl/status/exit_price ─────────────────────
    for leg_id, pnl, status, exit_price in leg_updates:
        sets = [f"pnl={pnl}", f"status='{status}'"]
        if is_eod:
            sets.append(f"exit_price={exit_price}")
        CLIENT.query(
            f"""
          UPDATE `{TRADE_LEGS_TABLE}`
          SET {', '.join(sets)}
          WHERE leg_id='{leg_id}'
        """
        )

    # ── 7️⃣ Aggregate per-trade and update trade_recommendations ──────────────────
    for tid, total in trade_totals.items():
        st = "'closed'" if is_eod else "'active'"
        exit_time_sql = "CURRENT_TIMESTAMP()" if is_eod else "exit_time"
        exit_price_sql = f"entry_price + {total}" if is_eod else "exit_price"

        CLIENT.query(
            f"""
          UPDATE `{TRADE_RECS_TABLE}`
          SET
            pnl       = {total},
            status    = {st},
            exit_time = {exit_time_sql}
            {', exit_price=' + exit_price_sql if is_eod else ''}
          WHERE trade_id='{tid}'
        """
        )

    logging.info("✅ PnL monitor complete (EOD=%s)", is_eod)
