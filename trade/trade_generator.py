# trade/trade_generator.py
# =====================
# Multi‑Strategy Trade Generator (0DTE Iron Condor & Vertical Spreads)
# =====================

import logging
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT

# Fully‑qualified BigQuery tables
TRADE_RECOMMENDATIONS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
TRADE_LEGS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
OPTION_SNAPSHOT_TABLE = f"{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot"
INDEX_PRICE_TABLE = f"{GOOGLE_CLOUD_PROJECT}.market_data.index_price_snapshot"

# Build a BigQuery client once
CREDENTIALS = get_gcp_credentials()
CLIENT = bigquery.Client(credentials=CREDENTIALS, project=GOOGLE_CLOUD_PROJECT)


def generate_0dte_trade(strategy_type: str = "iron_condor"):
    """
    Generate a 0DTE trade for the given strategy type.

    Steps:
      1) Fetch the latest SPX spot price.
      2) Determine today's expiration date.
      3) Load *only* the most recent mid_price+delta per strike from the
         snapshot table (UTC timestamps) using QUALIFY/ROW_NUMBER.
      4) Pick strikes for Iron Condor (10% delta, 10‑point wings) or
         an example vertical spread.
      5) Compute entry_price as net of leg mid‑prices.
      6) Insert into trade_recommendations and trade_legs.
    """
    logging.info(f"🔎 Generating 0DTE {strategy_type}...")

    try:
        # 1) Grab current SPX spot price
        price_q = f"""
            SELECT last
            FROM `{INDEX_PRICE_TABLE}`
            WHERE symbol = 'SPX'
            ORDER BY timestamp DESC
            LIMIT 1
        """
        price_df = CLIENT.query(price_q).to_dataframe()
        if price_df.empty:
            logging.warning("No SPX spot price available.")
            return
        spot_price = price_df["last"].iloc[0]

        # 2) Today's expiration date (0DTE)
        now_ts = datetime.now(timezone.utc)
        expiration_date = now_ts.date()

        # 3) Load only the *latest* snapshot per strike/option_type
        option_q = f"""
            SELECT
              strike,
              option_type,
              mid_price,
              delta
            FROM `{OPTION_SNAPSHOT_TABLE}`
            WHERE symbol = 'SPX'
              AND expiration_date = '{expiration_date}'
            QUALIFY
              ROW_NUMBER() OVER (
                PARTITION BY strike, option_type
                ORDER BY timestamp DESC
              ) = 1
        """
        options_df = CLIENT.query(option_q).to_dataframe()
        if options_df.empty:
            logging.warning("No 0DTE option data available for %s.", expiration_date)
            return

        # 4) Strategy logic: build legs list
        legs = []
        if strategy_type == "iron_condor":
            short_delta = 0.10  # 10% delta
            wing_width = 10  # 10‑point wings

            # separate calls vs puts
            puts = options_df[options_df.option_type == "put"]
            calls = options_df[options_df.option_type == "call"]

            # pick the nearest‐to‐delta strikes
            short_put_strike = puts[puts.delta.abs() <= short_delta].strike.min()
            short_call_strike = calls[calls.delta.abs() <= short_delta].strike.max()

            long_put_strike = short_put_strike - wing_width
            long_call_strike = short_call_strike + wing_width

            legs = [
                {"direction": "short", "type": "put", "strike": short_put_strike},
                {"direction": "long", "type": "put", "strike": long_put_strike},
                {"direction": "short", "type": "call", "strike": short_call_strike},
                {"direction": "long", "type": "call", "strike": long_call_strike},
            ]

        elif strategy_type == "vertical_spread":
            # Example: short 10% delta put + long wing_width below
            delta_target = 0.10
            wing_width = 10

            puts = options_df[options_df.option_type == "put"]
            sp = puts[puts.delta.abs() <= delta_target].strike.min()
            lp = sp - wing_width

            legs = [
                {"direction": "short", "type": "put", "strike": sp},
                {"direction": "long", "type": "put", "strike": lp},
            ]

        else:
            logging.error("Unknown strategy type: %s", strategy_type)
            return

        # 5) Build a lookup for mid_price by (strike, option_type)
        options_df.set_index(["strike", "option_type"], inplace=True)
        mid_map = options_df["mid_price"].to_dict()

        # 6) Compute entry_price = sum(long mid) − sum(short mid)
        entry_price = sum(
            mid_map[(leg["strike"], leg["type"])] * (1 if leg["direction"] == "long" else -1)
            for leg in legs
        )

        # 7) Create a unique trade_id
        trade_id = f"{strategy_type.upper()}_{now_ts.strftime('%Y%m%d%H%M%S')}"

        # 8) Insert into trade_recommendations
        CLIENT.insert_rows_json(
            TRADE_RECOMMENDATIONS_TABLE,
            [
                {
                    "trade_id": trade_id,
                    "strategy_type": strategy_type,
                    "symbol": "SPX",
                    "timestamp": now_ts,
                    "entry_time": now_ts,
                    "expiration_date": expiration_date,
                    "entry_price": entry_price,
                    "status": "active",
                    "notes": f"Auto‑generated 0DTE {strategy_type}",
                }
            ],
        )

        # 9) Insert each leg into trade_legs
        leg_rows = []
        for leg in legs:
            leg_id = f"{trade_id}_{leg['type'].upper()}_{leg['strike']}"
            leg_entry_price = mid_map[(leg["strike"], leg["type"])]
            leg_rows.append(
                {
                    "trade_id": trade_id,
                    "leg_id": leg_id,
                    "timestamp": now_ts,
                    "leg_type": leg["type"],
                    "direction": leg["direction"],
                    "strike": leg["strike"],
                    "entry_price": leg_entry_price,
                    "status": "open",
                    "notes": f"{leg['direction']} {leg['type']} @ {leg['strike']}",
                }
            )

        CLIENT.insert_rows_json(TRADE_LEGS_TABLE, leg_rows)
        logging.info("✅ Generated trade %s with legs %s", trade_id, legs)

    except Exception:
        logging.exception("❌ Error in generate_0dte_trade")
