# trade/pl_analysis.py
# =====================
# Compute and store P/L analysis for a given trade_id
# Refactored to accept legs_data and spot_price parameters.
# =====================

import logging
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
from google.cloud import bigquery

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT

# BigQuery table names
TRADE_LEGS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
OPTION_SNAPSHOT_TABLE = f"{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot"
TRADE_PL_ANALYSIS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_pl_analysis"
INDEX_PRICE_TABLE = f"{GOOGLE_CLOUD_PROJECT}.market_data.index_price_snapshot"

# One‐time client instantiation
CREDS = get_gcp_credentials()
CLIENT = bigquery.Client(credentials=CREDS, project=GOOGLE_CLOUD_PROJECT)


def compute_and_store_pl_analysis(
    trade_id: str,
    legs_data: Optional[pd.DataFrame] = None,
    spot_price: Optional[float] = None,
    grid_points: int = 200,
    underlying_range: float = 0.2,
):
    """
    For the given trade_id, compute and store P/L analysis.

    Args:
        trade_id: Identifier of the trade.
        legs_data: Optional DataFrame with columns ['leg_type','direction','strike','entry_price'].
                   If provided, skip querying BigQuery for leg info.
        spot_price: Optional current SPX spot price. If provided, skip querying BigQuery for spot.
        grid_points: Number of points in underlying price grid.
        underlying_range: Fractional range around spot for the grid.
    """
    logging.info("🔍 Computing P/L analysis for trade %s", trade_id)
    try:
        # 1️⃣ Load trade legs from passed-in data or BigQuery
        if legs_data is not None:
            legs_df = legs_data.copy()
            logging.debug("Using passed-in legs_data for trade %s", trade_id)
        else:
            # Fallback to BigQuery query if legs_data not supplied
            legs_sql = f"""
                SELECT leg_type, direction, strike, entry_price
                FROM `{TRADE_LEGS_TABLE}`
                WHERE trade_id = @tid
            """
            job_conf = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("tid", "STRING", trade_id)]
            )
            legs_df = CLIENT.query(legs_sql, job_config=job_conf).to_dataframe()
            if legs_df.empty:
                logging.warning("No legs found for trade %s, skipping P/L analysis", trade_id)
                return

        # 2️⃣ Determine SPX spot price from passed-in value or BigQuery
        if spot_price is not None:
            spot = spot_price
            logging.debug("Using passed-in spot_price for trade %s", trade_id)
        else:
            spot_sql = f"""
                SELECT last AS spot
                FROM `{INDEX_PRICE_TABLE}`
                WHERE symbol = 'SPX'
                ORDER BY timestamp DESC
                LIMIT 1
            """
            spot = CLIENT.query(spot_sql).to_dataframe()["spot"].iloc[0]

        # 3️⃣ Build underlying price grid around spot
        low, high = spot * (1 - underlying_range), spot * (1 + underlying_range)
        S = np.linspace(low, high, grid_points)  # underlying price axis

        # 4️⃣ Compute portfolio payoff at expiry, in index-points
        payoff = np.zeros_like(S)
        for _, leg in legs_df.iterrows():
            K = leg["strike"]
            ent = leg["entry_price"]
            sign = 1 if leg["direction"] == "long" else -1

            if leg["leg_type"] == "call":
                payoff += sign * np.maximum(S - K, 0)
            else:
                payoff += sign * np.maximum(K - S, 0)

            # subtract premium paid/received
            payoff -= sign * ent

        # Scale from points to dollars (contract multiplier = 100)
        payoff *= 100

        # 5️⃣ Compute key metrics in dollars
        max_profit = float(np.max(payoff))
        max_loss = float(np.min(payoff))

        # Breakeven points: where payoff crosses zero
        crossings = np.where(np.diff(np.sign(payoff)) != 0)[0]
        if crossings.size:
            breakeven_lower = float(S[crossings[0]])
            breakeven_upper = float(S[crossings[-1] + 1])
        else:
            breakeven_lower = breakeven_upper = float("nan")

        # Probability of profit: fraction of grid where P/L > 0
        prob_profit = float((payoff > 0).sum() / len(payoff) * 100)

        # 6️⃣ Fetch latest greeks for each leg via BigQuery
        greek_sql = f"""
            SELECT
              direction,
              delta * CASE WHEN direction='long' THEN 1 ELSE -1 END AS signed_delta,
              theta * CASE WHEN direction='long' THEN 1 ELSE -1 END AS signed_theta
            FROM (
              SELECT
                tl.direction,
                os.delta,
                os.theta,
                ROW_NUMBER() OVER (
                  PARTITION BY CAST(os.strike AS STRING), os.option_type
                  ORDER BY os.timestamp DESC
                ) AS rn
              FROM `{TRADE_LEGS_TABLE}` AS tl
              JOIN `{OPTION_SNAPSHOT_TABLE}` AS os
                ON tl.strike = os.strike
               AND tl.leg_type = os.option_type
              WHERE tl.trade_id = @tid
                AND os.symbol    = 'SPXW'
            ) AS sub
            WHERE sub.rn = 1
        """
        greeks_df = CLIENT.query(
            greek_sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("tid", "STRING", trade_id)]
            ),
        ).to_dataframe()
        tot_delta = float(greeks_df["signed_delta"].sum())
        tot_theta = float(greeks_df["signed_theta"].sum())

        # 7️⃣ Insert one row of P/L analysis into BigQuery
        now_iso = datetime.now(timezone.utc).isoformat()
        analysis_row = {
            "trade_id": trade_id,
            "timestamp": now_iso,
            "max_profit": max_profit,
            "max_loss": max_loss,
            "breakeven_lower": breakeven_lower,
            "breakeven_upper": breakeven_upper,
            "probability_profit": prob_profit,
            "delta": tot_delta,
            "theta": tot_theta,
            "notes": "auto‐computed via payoff grid (×100 multiplier)",
        }
        errors = CLIENT.insert_rows_json(TRADE_PL_ANALYSIS_TABLE, [analysis_row])
        if errors:
            logging.error("❌ P/L analysis insert errors for %s: %s", trade_id, errors)
            return

        logging.info(
            "✅ Stored P/L analysis for %s: profit=%.2f, loss=%.2f",
            trade_id,
            max_profit,
            max_loss,
        )

    except Exception:
        logging.exception("❌ Failed to compute P/L analysis for %s", trade_id)
