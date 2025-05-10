# trade/pnl_monitor.py
# =====================
# PnL Monitoring: snapshot legs → live_trade_pnl,
# then per‐row UPDATE of trade_legs & trade_recommendations.
# =====================

import logging
from datetime import datetime, timezone

import pytz
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT, INDEX_PRICE_TABLE_ID

# ── Table names ───────────────────────────────────────────────────────────────
TRADE_LEGS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
TRADE_RECS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
OPTION_SNAP = f"{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot"
LIVE_PNL = f"{GOOGLE_CLOUD_PROJECT}.analytics.live_trade_pnl"
PL_ANALYSIS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_pl_analysis"

# ── Client ─────────────────────────────────────────────────────────────────────
CLIENT = bigquery.Client(
    credentials=get_gcp_credentials(),
    project=GOOGLE_CLOUD_PROJECT,
)


def update_trade_pnl():
    """
    1) Snapshot each open leg’s current mid & raw PnL into live_trade_pnl.
    2) Per‐leg UPDATE of pnl, status, exit_price (at EOD) in trade_legs.
    3) Per‐trade sum PnL * 100 intraday, or override with max_profit/max_loss at EOD.
    4) Per‐trade UPDATE of pnl, status, exit_time, exit_price in trade_recommendations.
       (Closed trades are never re‐opened.)
    """
    # ── 0️⃣ Determine “now” in UTC & ET, and EOD window ─────────────────────────
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(pytz.timezone("US/Eastern"))
    # treat any call between 16:00 and 16:04 ET as EOD
    is_eod = now_et.hour == 16 and 0 <= now_et.minute < 5
    now_iso = now_utc.isoformat()

    # ── 1️⃣ Fetch all legs still marked “open” ──────────────────────────────────
    legs_df = CLIENT.query(
        f"""
        SELECT trade_id, leg_id, strike, leg_type, direction, entry_price
        FROM `{TRADE_LEGS}`
        WHERE status = 'open'
        """
    ).to_dataframe()

    if legs_df.empty:
        logging.info("No open legs to process.")
        return

    # ── 2️⃣ Pull the very latest mid_price for each strike+type needed ──────────
    # build WHERE clause: (strike=X AND option_type='put') OR …
    conds = " OR ".join(
        f"(strike={r.strike} AND option_type='{r.leg_type}')"
        for _, r in legs_df[["strike", "leg_type"]].drop_duplicates().iterrows()
    )
    mids_df = CLIENT.query(
        f"""
        SELECT strike, option_type, mid_price
        FROM `{OPTION_SNAP}`
        WHERE timestamp = (SELECT MAX(timestamp) FROM `{OPTION_SNAP}`)
          AND root_symbol = 'SPXW'
          AND ({conds})
        """
    ).to_dataframe()
    mid_map = {(r.strike, r.option_type): r.mid_price for _, r in mids_df.iterrows()}

    # ── 3️⃣ Get current SPX spot price ──────────────────────────────────────────
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

    # Accumulators for per‐trade raw PnL
    trade_totals = {}

    # ── 4️⃣ For each open leg: compute its raw PnL, snapshot it, then UPDATE trade_legs
    for _, leg in legs_df.iterrows():
        key = (leg.strike, leg.leg_type)
        current = mid_map.get(key, leg.entry_price)  # fallback to entry_price if missing

        # short P/L = entry − current; long P/L = current − entry
        raw_pnl = float(
            (leg.entry_price - current) if leg.direction == "short" else (current - leg.entry_price)
        )

        # status flips to “closed” at EOD; carry on otherwise
        new_status = "closed" if is_eod else "open"
        exit_price = float(current) if is_eod else None

        # 4a) Snapshot row
        CLIENT.insert_rows_json(
            LIVE_PNL,
            [
                {
                    "trade_id": leg.trade_id,
                    "leg_id": leg.leg_id,
                    "timestamp": now_iso,
                    "current_price": float(current),
                    "theoretical_pnl": raw_pnl,
                    "mark_price": float(current),
                    "underlying_price": underlying,
                    "price_type": "mid",
                    "underlying_symbol": "SPX",
                    "status": new_status,
                }
            ],
        )

        # 4b) UPDATE this leg row
        #    only override exit_price if we're closing
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

        # 4c) accumulate raw PnL for the trade
        trade_totals[leg.trade_id] = trade_totals.get(leg.trade_id, 0.0) + raw_pnl

    # ── 5️⃣ Now roll‐up per‐trade and UPDATE trade_recommendations───────────────
    for tid, raw_sum in trade_totals.items():
        if is_eod:
            # at EOD, pull your precomputed max_profit / max_loss
            pl_df = CLIENT.query(
                f"""
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
                p_max = float(pl_df.at[0, "max_profit"])
                p_min = float(pl_df.at[0, "max_loss"])
                # fetch your two short strikes
                info = CLIENT.query(
                    f"""
                    SELECT direction, leg_type, strike
                    FROM `{TRADE_LEGS}`
                    WHERE trade_id = @tid
                    """,
                    job_config=QueryJobConfig(
                        query_parameters=[ScalarQueryParameter("tid", "STRING", tid)]
                    ),
                ).to_dataframe()
                sp = float(info.query("direction=='short' and leg_type=='put'")["strike"].iloc[0])
                sc = float(info.query("direction=='short' and leg_type=='call'")["strike"].iloc[0])
                # between shorts → max_profit; else → max_loss
                final_pnl = p_max if (sp <= underlying <= sc) else p_min
            else:
                final_pnl = raw_sum * 100.0
            new_status = "closed"
            exit_price_sql = "entry_price + @pnl"
            exit_time_sql = "CURRENT_TIMESTAMP()"
        else:
            # intraday, just raw_sum × 100
            final_pnl = raw_sum * 100.0
            new_status = "active"
            exit_price_sql = "exit_price"
            exit_time_sql = "exit_time"

        # 5a) UPDATE trade_recommendations (skip any already closed)
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
