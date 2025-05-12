# trade/pnl_monitor.py
# =====================
# PnL Monitoring: compute & persist live vs end‑of‑day PnL
# for each symbol’s open trades. Supports any SUPPORTED_SYMBOLS.
# =====================

import logging
from datetime import datetime, timezone
from typing import Dict, Tuple

import pytz
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT

# ── BigQuery table identifiers ──────────────────────────────────────────────
TRADE_LEGS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
TRADE_RECS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
LIVE_PNL = f"{GOOGLE_CLOUD_PROJECT}.analytics.live_trade_pnl"
PL_ANALYSIS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_pl_analysis"

# ── One shared BigQuery client for all operations ────────────────────────────
CLIENT = bigquery.Client(
    credentials=get_gcp_credentials(),
    project=GOOGLE_CLOUD_PROJECT,
)

# ── Use same timezone for EOD detection ──────────────────────────────────────
NY_TZ = pytz.timezone("America/New_York")


def update_trade_pnl(symbol: str, quote: dict, mid_maps: Dict[str, Dict[Tuple[float, str], float]]):
    """
    Compute and persist PnL for each open option‐leg on a given symbol.

    Args:
      symbol    (str): Underlying ticker, e.g. "SPX" or "QQQ".
      quote     (dict): Latest underlying quote; must include key 'last' for price.
      mid_maps  (dict): Per‐expiry lookup of {(strike, option_type): mid_price}.

    Behavior:
      - Runs every 5 min; does intraday marking (“active” status).
      - Runs once at EOD (16:00–16:04 ET); does final close (“closed” status),
        writes exit_price & exit_time, and uses analytical PL if available.
    """
    # ── 0) Timestamps & EOD detection ────────────────────────────────────────
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(NY_TZ)
    # EOD window = any time from 16:00:00–16:04:59 ET
    is_eod = now_et.hour == 16 and now_et.minute < 5
    now_iso = now_utc.isoformat()  # BigQuery TIMESTAMP literal

    # ── 1) Fetch all open legs + their trade metadata ───────────────────────
    legs_sql = """
    SELECT
      t.trade_id,
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

    # If there are no open legs for this symbol, nothing more to do
    if legs_df.empty:
        logging.info("[%s] No open legs to process.", symbol)
        return

    # ── 2) Ensure we have a valid underlying quote ───────────────────────────
    if not quote or "last" not in quote:
        logging.warning("[%s] Missing underlying quote; skipping PnL update.", symbol)
        return
    underlying_price = float(quote["last"])  # index‐points

    # Accumulator for per‐trade raw PnL (in points)
    trade_totals: Dict[str, float] = {}

    # ── 3) Per‐leg PnL calculation, snapshot, and writeback ─────────────────
    for _, leg in legs_df.iterrows():
        exp_date = leg.expiration_date

        # 3a) Determine current mid price or fallback
        per_exp_map = mid_maps.get(exp_date, {})
        if is_eod:
            # At EOD: any missing mid_price → leg is OTM → price=0.0
            current_price = per_exp_map.get((leg.strike, leg.leg_type), 0.0)
        else:
            # Intraday: missing mid → assume flat since entry → price = entry_price
            current_price = per_exp_map.get((leg.strike, leg.leg_type), leg.entry_price)

        # 3b) Compute raw PnL in index‐points
        #   short position: entry_price - current_price
        #   long  position: current_price - entry_price
        raw_pts = (
            leg.entry_price - current_price
            if leg.direction == "short"
            else current_price - leg.entry_price
        )
        raw_pts = float(raw_pts)

        # 3c) Insert a live PnL snapshot row
        CLIENT.insert_rows_json(
            LIVE_PNL,
            [
                {
                    "trade_id": leg.trade_id,
                    "leg_id": leg.leg_id,
                    "timestamp": now_iso,
                    "current_price": current_price,
                    "theoretical_pnl": raw_pts,
                    "mark_price": current_price,
                    "underlying_price": underlying_price,
                    "price_type": "mid",
                    "underlying_symbol": symbol,
                    "status": "closed" if is_eod else "open",
                }
            ],
        )

        # 3d) Update the trade_legs table with fresh PnL & status
        leg_params = [
            ScalarQueryParameter("leg_id", "STRING", leg.leg_id),
            ScalarQueryParameter("pnl", "FLOAT", raw_pts),
            ScalarQueryParameter("cp", "FLOAT", current_price),
            ScalarQueryParameter("ts", "TIMESTAMP", now_iso),
        ]
        # Build SET clauses
        set_clauses = [
            "pnl = @pnl",
            f"status = '{'closed' if is_eod else 'open'}'",
        ]
        if is_eod:
            # Record final closing price & time
            set_clauses += [
                "exit_price = @cp",
                "exit_time  = @ts",
            ]
        CLIENT.query(
            f"""
            UPDATE `{TRADE_LEGS}`
            SET {', '.join(set_clauses)}
            WHERE leg_id = @leg_id
            """,
            job_config=QueryJobConfig(query_parameters=leg_params),
        )

        # 3e) Accumulate raw points for this leg’s trade
        trade_totals[leg.trade_id] = trade_totals.get(leg.trade_id, 0.0) + raw_pts

    # ── 4) Roll up per‐trade PnL and update recommendations ──────────────────
    for tid, sum_pts in trade_totals.items():
        if is_eod:
            # 4a) At EOD, prefer precomputed analytics if present
            pl_df = CLIENT.query(
                """
                SELECT max_profit, max_loss
                FROM `{PL_ANALYSIS}`
                WHERE trade_id = @tid
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                job_config=QueryJobConfig(
                    query_parameters=[ScalarQueryParameter("tid", "STRING", tid)]
                ),
            ).to_dataframe()

            if not pl_df.empty:
                # Use the analytic band to decide final payoff
                p_max = float(pl_df.at[0, "max_profit"])
                p_min = float(pl_df.at[0, "max_loss"])
                # Fetch the two short strikes to bracket the underlying
                info_df = CLIENT.query(
                    """
                    SELECT direction, leg_type, strike
                    FROM `{TRADE_LEGS}`
                    WHERE trade_id = @tid
                    """,
                    job_config=QueryJobConfig(
                        query_parameters=[ScalarQueryParameter("tid", "STRING", tid)]
                    ),
                ).to_dataframe()
                sp = float(
                    info_df.query("direction=='short' and leg_type=='put'")["strike"].iloc[0]
                )
                sc = float(
                    info_df.query("direction=='short' and leg_type=='call'")["strike"].iloc[0]
                )
                final_pnl = p_max if (sp <= underlying_price <= sc) else p_min
            else:
                # Fallback: sum of raw points * 100 contracts
                final_pnl = sum_pts * 100.0

            new_status = "closed"
        else:
            # 4b) Intraday: keep marked active, scale to contract size
            final_pnl = sum_pts * 100.0
            new_status = "active"

        # 4c) Update the trade_recommendations row
        rec_params = [
            ScalarQueryParameter("tid", "STRING", tid),
            ScalarQueryParameter("pnl", "FLOAT", final_pnl),
            ScalarQueryParameter("status", "STRING", new_status),
        ]
        exit_price_expr = "@cp" if is_eod else "exit_price"
        exit_time_expr = "@ts" if is_eod else "exit_time"

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
            job_config=QueryJobConfig(query_parameters=leg_params + rec_params),
        )

    logging.info("[%s] PnL monitor complete (EOD=%s)", symbol, is_eod)
