# trade/pnl_monitor.py
# =====================
# PnL Monitoring: snapshot legs → live_trade_pnl,
# then per‑row UPDATE of trade_legs & trade_recommendations.
# Refactored to fetch latest mid prices and underlying spot via Tradier API.
# =====================

import logging
from datetime import datetime, timezone

import pytz
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT, SPX
from fetcher.fetcher import fetch_option_chain, fetch_underlying_quote

# Table names
TRADE_LEGS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
TRADE_RECS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
LIVE_PNL = f"{GOOGLE_CLOUD_PROJECT}.analytics.live_trade_pnl"
PL_ANALYSIS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_pl_analysis"

# BigQuery client
CLIENT = bigquery.Client(credentials=get_gcp_credentials(), project=GOOGLE_CLOUD_PROJECT)


def update_trade_pnl():
    """
    1) Snapshot each open leg’s current mid & raw PnL into live_trade_pnl.
    2) Update trade_legs & trade_recommendations using latest data fetched via Tradier API.
    """
    # ── 0️⃣ Get current time and determine EOD window ─────────────────────────
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(pytz.timezone("US/Eastern"))
    is_eod = now_et.hour == 16 and 0 <= now_et.minute < 5
    now_iso = now_utc.isoformat()

    # ── 1️⃣ Fetch all open legs along with their expiration_date ────────────────
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
    """
    legs_df = CLIENT.query(legs_sql).to_dataframe()
    if legs_df.empty:
        logging.info("No open legs to process.")
        return

    # ── 2️⃣ Fetch current underlying quote and spot price via API ──────────────
    quote = fetch_underlying_quote(SPX)
    underlying = quote.get("last")
    if underlying is None:
        logging.warning("⚠️ Missing underlying price from API; aborting.")
        return

    # ── 3️⃣ For each unique expiration_date, fetch option chain and build mid_map ─
    mid_maps = {}
    for expiry in legs_df["expiration_date"].unique():
        # fetch full chain (200 strikes around current price)
        opts = fetch_option_chain(SPX, expiry, quote)
        # determine which (strike,option_type) pairs we need
        needed = set(
            (row.strike, row.leg_type)
            for _, row in legs_df[legs_df.expiration_date == expiry].iterrows()
        )
        # build a mapping of mid_price for needed legs
        mid_map = {}
        for o in opts:
            key = (o["strike"], o["option_type"])
            if key in needed:
                mid_map[key] = (o["bid"] + o["ask"]) / 2.0
        mid_maps[expiry] = mid_map

    # ── 4️⃣ Iterate over each leg: snapshot PnL and update trade_legs ──────────
    trade_totals = {}
    for _, leg in legs_df.iterrows():
        key = (leg.strike, leg.leg_type)
        # get current mid price or fallback to entry_price
        current = mid_maps.get(leg.expiration_date, {}).get(key, leg.entry_price)

        # compute raw PnL (short: entry - current, long: current - entry)
        raw_pnl = (
            leg.entry_price - current if leg.direction == "short" else current - leg.entry_price
        )
        raw_pnl = float(raw_pnl)

        # determine status and exit_price at EOD
        new_status = "closed" if is_eod else "open"
        exit_price = float(current) if is_eod else None

        # 4a) Snapshot into live_trade_pnl
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
                    "underlying_symbol": SPX,
                    "status": new_status,
                }
            ],
        )

        # 4b) UPDATE trade_legs table
        set_clauses = [f"pnl = {raw_pnl}", f"status = '{new_status}'"]
        if is_eod:
            set_clauses.append(f"exit_price = {exit_price}")
        CLIENT.query(
            f"""
            UPDATE `{TRADE_LEGS}`
            SET {', '.join(set_clauses)}
            WHERE leg_id = @leg_id
            """,
            job_config=QueryJobConfig(
                query_parameters=[ScalarQueryParameter("leg_id", "STRING", leg.leg_id)]
            ),
        )

        # accumulate raw PnL per trade
        trade_totals[leg.trade_id] = trade_totals.get(leg.trade_id, 0.0) + raw_pnl

    # ── 5️⃣ Roll up per‐trade and UPDATE trade_recommendations ─────────────────
    for tid, raw_sum in trade_totals.items():
        if is_eod:
            # at EOD, fetch precomputed max_profit/max_loss from PL_ANALYSIS
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
                    query_parameters=[ScalarQueryParameter("tid", "STRING", tid)]
                ),
            ).to_dataframe()
            if not pl_df.empty:
                p_max = float(pl_df.at[0, "max_profit"])
                p_min = float(pl_df.at[0, "max_loss"])
                # fetch short strikes to decide final PnL
                info_sql = f"""
                    SELECT direction, leg_type, strike
                    FROM `{TRADE_LEGS}`
                    WHERE trade_id = @tid
                """
                info_df = CLIENT.query(
                    info_sql,
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
                # in‑between shorts → max_profit, else → max_loss
                final_pnl = p_max if (sp <= underlying <= sc) else p_min
            else:
                final_pnl = raw_sum * 100.0
            new_status = "closed"
            exit_price_sql = "entry_price + @pnl"
            exit_time_sql = "CURRENT_TIMESTAMP()"
        else:
            # intraday: simple raw_sum × 100
            final_pnl = raw_sum * 100.0
            new_status = "active"
            exit_price_sql = "exit_price"
            exit_time_sql = "exit_time"

        # 5a) UPDATE trade_recommendations
        CLIENT.query(
            f"""
            UPDATE `{TRADE_RECS}`
            SET
              pnl        = @pnl,
              status     = @status,
              exit_price = {exit_price_sql},
              exit_time  = {exit_time_sql}
            WHERE trade_id = @tid
              AND status != 'closed'
            """,
            job_config=QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("tid", "STRING", tid),
                    ScalarQueryParameter("pnl", "FLOAT", final_pnl),
                    ScalarQueryParameter("status", "STRING", new_status),
                ]
            ),
        )

    logging.info("✅ PnL monitor complete (EOD=%s)", is_eod)
