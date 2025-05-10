# trade/pnl_monitor.py
# =====================
# PnL Monitoring: compute & persist live and end‑of‑day PnL for each symbol’s open trades.
# Supports any list of SUPPORTED_SYMBOLS by filtering on `symbol`.
# =====================

import logging
from datetime import datetime, timezone
from typing import Dict, Tuple

import pytz
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT

# ── Fully qualified BigQuery table names ─────────────────────────────────────
TRADE_LEGS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
TRADE_RECS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
LIVE_PNL = f"{GOOGLE_CLOUD_PROJECT}.analytics.live_trade_pnl"
PL_ANALYSIS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_pl_analysis"

# ── Initialize one BigQuery client for all operations ─────────────────────────
CLIENT = bigquery.Client(
    credentials=get_gcp_credentials(),
    project=GOOGLE_CLOUD_PROJECT,
)

# ── Use the same NY timezone for EOD detection ────────────────────────────────
NY_TZ = pytz.timezone("America/New_York")


def update_trade_pnl(
    symbol: str, quote: dict = None, mid_maps: Dict[str, Dict[Tuple[float, str], float]] = None
):
    """
    Compute and update PnL for all open trades of a given symbol.

    Args:
      symbol   (str): e.g. "SPX" or "QQQ"
      quote    (dict): underlying quote dict from Tradier, must contain 'last'
      mid_maps (dict): {
                         expiration_date -> {
                           (strike, option_type) -> mid_price
                         }
                       }
                       if None, fall back to entry price.
    """
    # ── 0️⃣  Determine current time in UTC & ET ─────────────────────────────
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(NY_TZ)
    # Treat 16:00–16:04 ET as end‑of‑day closure window
    is_eod = now_et.hour == 16 and now_et.minute < 5
    now_iso = now_utc.isoformat()

    # ── 1️⃣  Load all open legs for THIS symbol, joining on recommendation info ─
    legs_sql = f"""
        SELECT t.trade_id,
               t.leg_id,
               t.strike,
               t.leg_type,
               t.direction,
               t.entry_price,
               r.expiration_date
        FROM `{TRADE_LEGS}` AS t
        JOIN `{TRADE_RECS}` AS r USING(trade_id)
        WHERE t.status = 'open'
          AND r.symbol = @symbol
    """
    legs_df = CLIENT.query(
        legs_sql,
        job_config=QueryJobConfig(
            query_parameters=[ScalarQueryParameter("symbol", "STRING", symbol)]
        ),
    ).to_dataframe()

    # If no open legs found, nothing to do
    if legs_df.empty:
        logging.info("[%s] No open legs to process.", symbol)
        return

    # ── 2️⃣  Validate underlying quote ───────────────────────────────────────
    if not quote or "last" not in quote:
        logging.warning("[%s] Missing underlying quote; skipping PnL update.", symbol)
        return
    underlying = float(quote["last"])

    # Will accumulate raw PnL (in points) per trade_id
    trade_totals: Dict[str, float] = {}

    # ── 3️⃣  Iterate through each leg and snapshot & update PnL ─────────────
    for _, leg in legs_df.iterrows():
        exp_date = leg.expiration_date
        # Look up mid map for this expiry; default to empty
        exp_map = (mid_maps or {}).get(exp_date, {})
        # Find current mid price or fallback to the entry price
        current = exp_map.get((leg.strike, leg.leg_type), leg.entry_price)

        # Compute raw PnL in points: short = entry - current; long = current - entry
        raw_pnl = (
            leg.entry_price - current if leg.direction == "short" else (current - leg.entry_price)
        )
        raw_pnl = float(raw_pnl)

        # Determine new status & exit_price if EOD
        status = "closed" if is_eod else "open"
        exit_price = float(current) if is_eod else None

        # ── 3a) Insert a live PnL snapshot into LIVE_PNL table ─────────────
        CLIENT.insert_rows_json(
            LIVE_PNL,
            [
                {
                    "trade_id": leg.trade_id,
                    "leg_id": leg.leg_id,
                    "timestamp": now_iso,
                    "current_price": current,
                    "theoretical_pnl": raw_pnl,
                    "mark_price": current,
                    "underlying_price": underlying,
                    "price_type": "mid",
                    "underlying_symbol": symbol,
                    "status": status,
                }
            ],
        )

        # ── 3b) Update the trade_legs record with new PnL & status ─────────
        set_parts = [f"pnl = {raw_pnl}", f"status = '{status}'"]
        if is_eod:
            set_parts.append(f"exit_price = {exit_price}")
        CLIENT.query(
            f"""
            UPDATE `{TRADE_LEGS}`
            SET {', '.join(set_parts)}
            WHERE leg_id = @leg_id
            """,
            job_config=QueryJobConfig(
                query_parameters=[ScalarQueryParameter("leg_id", "STRING", leg.leg_id)]
            ),
        )

        # Accumulate per-trade raw PnL
        trade_totals[leg.trade_id] = trade_totals.get(leg.trade_id, 0.0) + raw_pnl

    # ── 4️⃣  Roll up per-trade totals into trade_recommendations ────────────
    for trade_id, raw_sum in trade_totals.items():
        if is_eod:
            # At EOD, possibly use precomputed max_profit/max_loss
            pl_sql = f"""
                SELECT max_profit, max_loss
                FROM `{PL_ANALYSIS}`
                WHERE trade_id = @tid
                ORDER BY timestamp DESC
                LIMIT 1
            """
            pl_df = CLIENT.query(
                pl_sql,
                job_config=QueryJobConfig(
                    query_parameters=[ScalarQueryParameter("tid", "STRING", trade_id)]
                ),
            ).to_dataframe()

            if not pl_df.empty:
                p_max = float(pl_df.at[0, "max_profit"])
                p_min = float(pl_df.at[0, "max_loss"])
                # Fetch short strikes to decide final payoff
                info_sql = f"""
                    SELECT direction, leg_type, strike
                    FROM `{TRADE_LEGS}`
                    WHERE trade_id = @tid
                """
                info_df = CLIENT.query(
                    info_sql,
                    job_config=QueryJobConfig(
                        query_parameters=[ScalarQueryParameter("tid", "STRING", trade_id)]
                    ),
                ).to_dataframe()
                sp = float(
                    info_df.query("direction=='short' and leg_type=='put'")["strike"].iloc[0]
                )
                sc = float(
                    info_df.query("direction=='short' and leg_type=='call'")["strike"].iloc[0]
                )
                # Final = max_profit if underlying in bracket, else max_loss
                final_pnl = p_max if (sp <= underlying <= sc) else p_min
            else:
                # Fallback to raw_sum * contract size (100)
                final_pnl = raw_sum * 100.0

            # EOD exit expressions for SQL update
            exit_price_expr = "entry_price + @pnl"
            exit_time_expr = "CURRENT_TIMESTAMP()"
            new_status = "closed"
        else:
            # Intraday: mark PnL as raw_sum * 100, leave status active
            final_pnl = raw_sum * 100.0
            new_status = "active"
            exit_price_expr = "exit_price"
            exit_time_expr = "exit_time"

        # ── 4a) Update the trade_recommendations record ─────────────────────
        CLIENT.query(
            f"""
            UPDATE `{TRADE_RECS}`
            SET
              pnl        = @pnl,
              status     = @status,
              exit_price = {exit_price_expr},
              exit_time  = {exit_time_expr}
            WHERE trade_id = @tid
              AND status != 'closed'
            """,
            job_config=QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("tid", "STRING", trade_id),
                    ScalarQueryParameter("pnl", "FLOAT", final_pnl),
                    ScalarQueryParameter("status", "STRING", new_status),
                ]
            ),
        )

    logging.info("[%s] PnL monitor complete (EOD=%s)", symbol, is_eod)
