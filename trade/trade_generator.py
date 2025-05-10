# trade/trade_generator.py
# =====================
# Multi‑Strategy Trade Generator (0DTE Iron Condor & Vertical Spreads)
# Always uses mid‑prices from your option_chain_snapshot for entry P/L math.
# Refactored to pass legs_data and spot_price to P/L analysis to avoid redundant queries.
# =====================

import logging
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from common.auth import get_gcp_credentials
from common.config import (
    GOOGLE_CLOUD_PROJECT,
    MODEL_VERSION,
    TARGET_DELTA,
    WING_WIDTH,
)
from trade.pl_analysis import compute_and_store_pl_analysis

# Fully‑qualified BigQuery tables
TRADE_RECS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
TRADE_LEGS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
OPT_SNAP = f"{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot"
IDX_PRICE = f"{GOOGLE_CLOUD_PROJECT}.market_data.index_price_snapshot"

# Instantiate a BigQuery client once
CLIENT = bigquery.Client(credentials=get_gcp_credentials(), project=GOOGLE_CLOUD_PROJECT)


def _closest_strike(available: pd.Series, target: float) -> float:
    """
    Return the strike in `available` closest to `target`.
    """
    idx = (available - target).abs().idxmin()
    return available.loc[idx]


def generate_0dte_trade(strategy_type: str = "iron_condor"):
    """
    1) Fetch SPX spot.
    2) Pull the freshest option snapshot per (strike, option_type).
    3) Pick strikes for Iron Condor or Vertical Spread.
    4) Always compute entry mid‑prices as (bid+ask)/2.
    5) Sum signed mid‑prices → net entry_price.
    6) Insert into trade_recommendations & trade_legs.
    7) Trigger P/L analysis, passing data to avoid redundant BQ queries.
    """
    logging.info("🔎 Generating 0DTE %s...", strategy_type)

    try:
        # 1️⃣ Get the latest SPX spot price
        price_sql = f"""
          SELECT last
          FROM `{IDX_PRICE}`
          WHERE symbol = 'SPX'
          ORDER BY timestamp DESC
          LIMIT 1
        """
        spot_df = CLIENT.query(price_sql).to_dataframe()
        if spot_df.empty:
            logging.warning("No SPX spot price; aborting.")
            return
        spot = spot_df["last"].iloc[0]

        # Record timestamps
        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.isoformat()
        expiry = now_dt.date().isoformat()

        # 2️⃣ Freshest snapshot per strike+option_type
        opts_sql = f"""
          SELECT strike,
                 option_type,
                 bid,
                 ask,
                 delta
          FROM (
            SELECT
              strike,
              option_type,
              bid,
              ask,
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
        job_conf = QueryJobConfig(query_parameters=[ScalarQueryParameter("expiry", "DATE", expiry)])
        opts_df = CLIENT.query(opts_sql, job_config=job_conf).to_dataframe()
        if opts_df.empty:
            logging.warning("No 0DTE options for %s; aborting.", expiry)
            return

        # 3️⃣ Choose strikes according to strategy
        puts = opts_df[opts_df.option_type == "put"].copy()
        calls = opts_df[opts_df.option_type == "call"].copy()
        legs = []

        if strategy_type == "iron_condor":
            if puts.empty or calls.empty:
                logging.warning("Missing puts/calls; skipping.")
                return
            # find strikes closest to TARGET_DELTA
            puts["dd"] = (puts.delta.abs() - TARGET_DELTA).abs()
            calls["dd"] = (calls.delta.abs() - TARGET_DELTA).abs()

            short_put_strike = puts.loc[puts.dd.idxmin(), "strike"]
            short_call_strike = calls.loc[calls.dd.idxmin(), "strike"]
            long_put_strike = _closest_strike(puts.strike, short_put_strike - WING_WIDTH)
            long_call_strike = _closest_strike(calls.strike, short_call_strike + WING_WIDTH)

            legs = [
                {"direction": "short", "type": "put", "strike": short_put_strike},
                {"direction": "long", "type": "put", "strike": long_put_strike},
                {"direction": "short", "type": "call", "strike": short_call_strike},
                {"direction": "long", "type": "call", "strike": long_call_strike},
            ]

        elif strategy_type == "vertical_spread":
            if puts.empty:
                logging.warning("No puts; skipping vertical spread.")
                return
            puts["dd"] = (puts.delta.abs() - TARGET_DELTA).abs()
            short_strike = puts.loc[puts.dd.idxmin(), "strike"]
            long_strike = _closest_strike(puts.strike, short_strike - WING_WIDTH)
            legs = [
                {"direction": "short", "type": "put", "strike": short_strike},
                {"direction": "long", "type": "put", "strike": long_strike},
            ]

        else:
            logging.error("Unknown strategy_type: %s", strategy_type)
            return

        # 4️⃣ Build mid-price mapping
        opts_df["mid_price"] = (opts_df["bid"] + opts_df["ask"]) / 2.0
        price_map = opts_df.set_index(["strike", "option_type"])["mid_price"].to_dict()

        # validate no missing prices
        missing = [L for L in legs if (L["strike"], L["type"]) not in price_map]
        if missing:
            logging.error("Missing mid_price for %s; aborting.", missing)
            return

        # 5️⃣ Compute net entry price (sum of signed mid-prices)
        entry_price = sum(
            price_map[(L["strike"], L["type"])].__mul__(1 if L["direction"] == "long" else -1)
            for L in legs
        )

        # 6️⃣ Insert into trade_recommendations
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
        errs = CLIENT.insert_rows_json(TRADE_RECS, [rec])
        if errs:
            logging.error("❌ Insert rec errors: %s", errs)
            return

        # 7️⃣ Insert each leg into trade_legs
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
                    "entry_price": price_map[(L["strike"], L["type"])],
                    "status": "open",
                    "notes": f"{L['direction']} {L['type']} @ {L['strike']}",
                }
            )
        errs = CLIENT.insert_rows_json(TRADE_LEGS, leg_rows)
        if errs:
            logging.error("❌ Insert legs errors: %s", errs)
            return

        logging.info("✅ Generated %s with legs %s", trade_id, legs)

        # 8️⃣ Prepare and pass data to P/L analysis to avoid redundant BigQuery queries
        # Build a DataFrame with only the necessary columns
        legs_df_for_analysis = pd.DataFrame(leg_rows)[
            [
                "leg_type",
                "direction",
                "strike",
                "entry_price",
            ]
        ]

        # Trigger P/L analysis, supplying legs_data and spot
        compute_and_store_pl_analysis(
            trade_id=trade_id, legs_data=legs_df_for_analysis, spot_price=spot
        )

    except Exception:
        logging.exception("❌ Error in generate_0dte_trade()")
