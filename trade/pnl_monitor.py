# =====================
# trade/pnl_monitor.py
# PnL Monitoring and Aggregation for Multi-Leg Strategies
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

CREDENTIALS = get_gcp_credentials()
CLIENT = bigquery.Client(credentials=CREDENTIALS, project=GOOGLE_CLOUD_PROJECT)


def update_trade_pnl():
    """
    Update PnL for each leg and aggregate at the trade level.
    """
    logging.info("Updating PnL for open trades...")

    try:
        # Fetch all open trade legs
        query = f"""
            SELECT trade_id, leg_id, strike, direction, entry_price, status
            FROM `{TRADE_LEGS_TABLE}`
            WHERE status = 'open'
        """
        legs_df = CLIENT.query(query).to_dataframe()

        if legs_df.empty:
            logging.info("No open legs to update.")
            return

        # Fetch latest mid prices for all strikes
        strike_query = f"""
            SELECT strike, mid_price
            FROM `{OPTION_SNAPSHOT_TABLE}`
            WHERE timestamp = (SELECT MAX(timestamp) FROM `{OPTION_SNAPSHOT_TABLE}`)
        """
        strike_prices = CLIENT.query(strike_query).to_dataframe()
        strike_price_map = strike_prices.set_index("strike")["mid_price"].to_dict()

        # Calculate PnL per leg and aggregate at the trade level
        trade_pnl_map = {}
        leg_updates = []

        for _, leg in legs_df.iterrows():
            strike = leg["strike"]
            leg_id = leg["leg_id"]
            direction = leg["direction"]
            entry_price = leg["entry_price"]
            trade_id = leg["trade_id"]

            current_price = strike_price_map.get(strike, entry_price)

            # Calculate PnL per leg
            pnl = (
                (entry_price - current_price)
                if direction == "short"
                else (current_price - entry_price)
            )

            # Update leg record
            leg_updates.append(
                {
                    "leg_id": leg_id,
                    "pnl": pnl,
                    "status": "closed" if datetime.now(timezone.utc).hour >= 16 else "open",
                }
            )

            # Aggregate PnL at the trade level
            trade_pnl_map[trade_id] = trade_pnl_map.get(trade_id, 0) + pnl

        # Update legs in BigQuery
        for leg_update in leg_updates:
            CLIENT.query(
                f"""
                UPDATE `{TRADE_LEGS_TABLE}`
                SET pnl = {leg_update['pnl']}, status = '{leg_update['status']}'
                WHERE leg_id = '{leg_update['leg_id']}'
            """
            )

        # Update trade PnL in `trade_recommendations`
        for trade_id, total_pnl in trade_pnl_map.items():
            CLIENT.query(
                f"""
                UPDATE `{TRADE_RECOMMENDATIONS_TABLE}`
                SET pnl = {total_pnl},
                    status = 'closed' if {datetime.now(timezone.utc).hour >= 16} ELSE 'active',
                    exit_time = CURRENT_TIMESTAMP()
                WHERE trade_id = '{trade_id}'
            """
            )

        logging.info("PnL update complete.")

    except Exception as e:
        logging.error(f"Error updating PnL: {e}")
