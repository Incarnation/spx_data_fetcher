# trade/trade_generator.py
# =====================
# Multi‑Strategy Trade Generator (0DTE Iron Condor & Vertical Spreads)
# =====================

import logging
import os
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT, MODEL_VERSION, TARGET_DELTA, WING_WIDTH
from trade.pl_analysis import compute_and_store_pl_analysis

# Fully‑qualified BigQuery tables
TRADE_RECS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
TRADE_LEGS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
OPT_SNAP = f"{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot"
IDX_PRICE = f"{GOOGLE_CLOUD_PROJECT}.market_data.index_price_snapshot"

# Instantiate a BigQuery client once
CREDS = get_gcp_credentials()
CLIENT = bigquery.Client(credentials=CREDS, project=GOOGLE_CLOUD_PROJECT)


def _closest_strike(available: pd.Series, target: float) -> float:
    """
    Return the strike in `available` closest to `target`.
    """
    idx = (available - target).abs().idxmin()
    return available.loc[idx]


def generate_0dte_trade(strategy_type: str = "iron_condor"):
    """
    Generate a 0DTE trade for `strategy_type`:
      1) Fetch SPX spot price.
      2) Build freshest options snapshot per (strike,option_type).
      3) Select strikes for Iron Condor or Vertical Spread.
      4) Compute net entry price.
      5) Insert into trade_recommendations & trade_legs.
      6) Trigger P/L analysis.
    """
    logging.info("🔎 Generating 0DTE %s...", strategy_type)

    try:
        # 1) SPX spot
        price_sql = f"""
          SELECT last
          FROM `{IDX_PRICE}`
          WHERE symbol = 'SPX'
          ORDER BY timestamp DESC
          LIMIT 1
        """
        price_df = CLIENT.query(price_sql).to_dataframe()
        if price_df.empty:
            logging.warning("No SPX spot price; aborting.")
            return
        spot = price_df["last"].iloc[0]

        # 2) Timestamps & expiration
        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.isoformat()
        expiry = now_dt.date().isoformat()

        # 3) Freshest per-(strike,option_type)
        opts_sql = f"""
          SELECT strike, option_type, mid_price, delta
          FROM (
            SELECT
              strike,
              option_type,
              mid_price,
              delta,
              ROW_NUMBER() OVER (
                PARTITION BY CAST(strike AS STRING), option_type
                ORDER BY timestamp DESC
              ) AS rn
            FROM `{OPT_SNAP}`
            WHERE root_symbol     = 'SPXW'
              AND expiration_date = @expiry
          )
          WHERE rn = 1
        """
        job_conf = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("expiry", "DATE", expiry)]
        )
        opts_df = CLIENT.query(opts_sql, job_config=job_conf).to_dataframe()
        if opts_df.empty:
            logging.warning("No 0DTE options for %s; aborting.", expiry)
            return

        # 4) Strike selection
        legs = []
        if strategy_type == "iron_condor":
            # split calls/puts
            puts = opts_df[opts_df.option_type == "put"].copy()
            calls = opts_df[opts_df.option_type == "call"].copy()
            if puts.empty or calls.empty:
                logging.warning("Missing puts/calls; skipping.")
                return

            # distance from target delta
            puts["dd"] = (puts.delta.abs() - TARGET_DELTA).abs()
            calls["dd"] = (calls.delta.abs() - TARGET_DELTA).abs()

            sp = puts.loc[puts.dd.idxmin(), "strike"]
            sc = calls.loc[calls.dd.idxmin(), "strike"]
            lp_i = sp - WING_WIDTH
            lc_i = sc + WING_WIDTH

            lp = _closest_strike(puts.strike, lp_i)
            lc = _closest_strike(calls.strike, lc_i)

            legs = [
                {"direction": "short", "type": "put", "strike": sp},
                {"direction": "long", "type": "put", "strike": lp},
                {"direction": "short", "type": "call", "strike": sc},
                {"direction": "long", "type": "call", "strike": lc},
            ]

        elif strategy_type == "vertical_spread":
            puts = opts_df[opts_df.option_type == "put"].copy()
            if puts.empty:
                logging.warning("No puts; skipping vertical spread.")
                return

            puts["dd"] = (puts.delta.abs() - TARGET_DELTA).abs()
            sp = puts.loc[puts.dd.idxmin(), "strike"]
            lp = _closest_strike(puts.strike, sp - WING_WIDTH)

            legs = [
                {"direction": "short", "type": "put", "strike": sp},
                {"direction": "long", "type": "put", "strike": lp},
            ]

        else:
            logging.error("Unknown strategy_type: %s", strategy_type)
            return

        # 5) Compute entry price
        opts_df.set_index(["strike", "option_type"], inplace=True)
        mid_map = opts_df["mid_price"].to_dict()
        missing = [leg for leg in legs if (leg["strike"], leg["type"]) not in mid_map]
        if missing:
            logging.error("Missing mid_price for %s; aborting.", missing)
            return
        entry_price = sum(
            mid_map[(L["strike"], L["type"])] * (1 if L["direction"] == "long" else -1)
            for L in legs
        )

        # 6) Insert into trade_recommendations
        trade_id = f"{strategy_type.upper()}_{now_dt.strftime('%Y%m%d%H%M%S')}"
        rec = {
            "trade_id": trade_id,
            "strategy_type": strategy_type,
            "symbol": "SPX",
            "timestamp": now_iso,
            "entry_time": now_iso,
            "expiration_date": expiry,
            "entry_price": entry_price,
            "status": "active",
            "model_version": MODEL_VERSION,
            "notes": f"Auto‑generated 0DTE {strategy_type}",
        }
        errors = CLIENT.insert_rows_json(TRADE_RECS, [rec])
        if errors:
            logging.error("❌ Insert rec errors: %s", errors)
            return

        # 7) Insert each leg
        leg_rows = []
        for L in legs:
            lid = f"{trade_id}_{L['type'].upper()}_{L['strike']}"
            leg_rows.append(
                {
                    "trade_id": trade_id,
                    "leg_id": lid,
                    "timestamp": now_iso,
                    "leg_type": L["type"],
                    "direction": L["direction"],
                    "strike": L["strike"],
                    "entry_price": mid_map[(L["strike"], L["type"])],
                    "status": "open",
                    "notes": f"{L['direction']} {L['type']} @ {L['strike']}",
                }
            )
        errors = CLIENT.insert_rows_json(TRADE_LEGS, leg_rows)
        if errors:
            logging.error("❌ Insert legs errors: %s", errors)
            return

        logging.info("✅ Generated %s with legs %s", trade_id, legs)

        # 8) Trigger P/L analysis
        compute_and_store_pl_analysis(trade_id)

    except Exception:
        logging.exception("❌ Error in generate_0dte_trade()")
