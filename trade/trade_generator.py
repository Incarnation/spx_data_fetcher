# =====================
# trade/trade_generator.py
# Multi-Strategy Trade Generator (0DTE)
# =====================

import logging
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT

TRADE_RECOMMENDATIONS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
TRADE_LEGS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
OPTION_SNAPSHOT_TABLE = f"{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot"
INDEX_PRICE_TABLE = f"{GOOGLE_CLOUD_PROJECT}.market_data.index_price_snapshot"

CREDENTIALS = get_gcp_credentials()
CLIENT = bigquery.Client(credentials=CREDENTIALS, project=GOOGLE_CLOUD_PROJECT)


def generate_0dte_trade(strategy_type: str = "iron_condor"):
    """
    Generate a 0DTE trade for the given strategy type (Iron Condor, Vertical Spread, etc.).
    """
    logging.info(f"Generating 0DTE trade for {strategy_type}...")

    try:
        # Get the current spot price for SPX
        price_query = f"""
            SELECT last
            FROM `{INDEX_PRICE_TABLE}`
            WHERE symbol = 'SPX'
            ORDER BY timestamp DESC
            LIMIT 1
        """
        spot_price = CLIENT.query(price_query).to_dataframe()["last"].iloc[0]

        # Get 0DTE expiration date
        expiration_date = datetime.now(timezone.utc).date()

        # Fetch option chain data
        option_query = f"""
            SELECT strike, option_type, mid_price, delta
            FROM `{OPTION_SNAPSHOT_TABLE}`
            WHERE symbol = 'SPX'
            AND expiration_date = '{expiration_date}'
        """
        options_df = CLIENT.query(option_query).to_dataframe()

        if options_df.empty:
            logging.warning("No option data available for SPX 0DTE.")
            return

        # Define structure based on strategy
        if strategy_type == "iron_condor":
            short_delta = 0.10
            wing_width = 10

            # Short strikes
            short_put_strike = options_df[
                (options_df["option_type"] == "put") & (options_df["delta"].abs() <= short_delta)
            ]["strike"].min()
            short_call_strike = options_df[
                (options_df["option_type"] == "call") & (options_df["delta"].abs() <= short_delta)
            ]["strike"].max()

            # Long strikes
            long_put_strike = short_put_strike - wing_width
            long_call_strike = short_call_strike + wing_width

            legs = [
                {"direction": "short", "type": "put", "strike": short_put_strike},
                {"direction": "long", "type": "put", "strike": long_put_strike},
                {"direction": "short", "type": "call", "strike": short_call_strike},
                {"direction": "long", "type": "call", "strike": long_call_strike},
            ]

        elif strategy_type == "vertical_spread":
            # Example Vertical Spread structure
            delta_target = 0.10
            wing_width = 10

            short_put_strike = options_df[
                (options_df["option_type"] == "put") & (options_df["delta"].abs() <= delta_target)
            ]["strike"].min()
            long_put_strike = short_put_strike - wing_width

            legs = [
                {"direction": "short", "type": "put", "strike": short_put_strike},
                {"direction": "long", "type": "put", "strike": long_put_strike},
            ]

        # Calculate entry price
        leg_prices = options_df[options_df["strike"].isin([leg["strike"] for leg in legs])]
        entry_price = sum(leg_prices["mid_price"])

        # Create trade ID
        trade_id = f"{strategy_type.upper()}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Insert into trade_recommendations
        CLIENT.insert_rows_json(
            TRADE_RECOMMENDATIONS_TABLE,
            [
                {
                    "trade_id": trade_id,
                    "strategy_type": strategy_type,
                    "symbol": "SPX",
                    "timestamp": datetime.now(timezone.utc),
                    "entry_time": datetime.now(timezone.utc),
                    "expiration_date": expiration_date,
                    "entry_price": entry_price,
                    "status": "active",
                    "notes": f"Generated 0DTE {strategy_type} trade.",
                }
            ],
        )

        # Insert into trade_legs
        leg_rows = []
        for leg in legs:
            leg_id = f"{trade_id}_{leg['type'].upper()}_{leg['strike']}"
            leg_row = {
                "trade_id": trade_id,
                "leg_id": leg_id,
                "timestamp": datetime.now(timezone.utc),
                "leg_type": leg["type"],
                "direction": leg["direction"],
                "strike": leg["strike"],
                "entry_price": entry_price,
                "status": "open",
                "notes": f"Leg of {strategy_type} trade.",
            }
            leg_rows.append(leg_row)

        CLIENT.insert_rows_json(TRADE_LEGS_TABLE, leg_rows)

        logging.info(f"Generated {strategy_type} trade: {trade_id}")

    except Exception as e:
        logging.error(f"Error generating trade: {e}")
