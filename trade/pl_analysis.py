# trade/pl_analysis.py
# =====================
# Compute and store P/L analysis for a given trade_id
# =====================

import logging
from datetime import datetime, timezone

import numpy as np
from google.cloud import bigquery

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT

# Tables
TRADE_LEGS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
OPT_SNAP = f"{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot"
TRADE_PL_ANALYSIS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_pl_analysis"
IDX_PRICE = f"{GOOGLE_CLOUD_PROJECT}.market_data.index_price_snapshot"

# Client
CREDS = get_gcp_credentials()
CLIENT = bigquery.Client(credentials=CREDS, project=GOOGLE_CLOUD_PROJECT)


def compute_and_store_pl_analysis(
    trade_id: str, grid_points: int = 200, underlying_range: float = 0.2
):
    """
    For `trade_id`:
      1) Load its legs.
      2) Build payoff grid at expiry.
      3) Compute max_profit, max_loss, breakevens, PoP.
      4) Sum signed Δ & Θ from freshest snapshot.
      5) Write results into analytics.trade_pl_analysis.
    """
    logging.info("🔍 Computing P/L analysis for %s", trade_id)
    try:
        # 1️⃣ Fetch legs
        legs_sql = """
          SELECT leg_id, leg_type, direction, strike, entry_price
          FROM `{TL}`
          WHERE trade_id = @tid
        """.format(
            TL=TRADE_LEGS
        )
        job_conf = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("tid", "STRING", trade_id)]
        )
        legs = CLIENT.query(legs_sql, job_config=job_conf).to_dataframe()
        if legs.empty:
            logging.warning("No legs for %s", trade_id)
            return

        # 2️⃣ Fetch spot
        spot_sql = f"""
          SELECT last AS spot
          FROM `{IDX_PRICE}`
          WHERE symbol = 'SPX'
          ORDER BY timestamp DESC
          LIMIT 1
        """
        spot = CLIENT.query(spot_sql).to_dataframe()["spot"].iloc[0]

        # 3️⃣ Underlying grid
        low, high = spot * (1 - underlying_range), spot * (1 + underlying_range)
        S = np.linspace(low, high, grid_points)

        # 4️⃣ Payoff
        payoff = np.zeros_like(S)
        for _, L in legs.iterrows():
            K = L["strike"]
            ent = L["entry_price"]
            sign = 1 if L["direction"] == "long" else -1
            if L["leg_type"] == "call":
                payoff += sign * np.maximum(S - K, 0)
            else:
                payoff += sign * np.maximum(K - S, 0)
            payoff -= sign * ent

        max_profit = float(np.max(payoff))
        max_loss = float(np.min(payoff))
        crosses = np.where(np.diff(np.sign(payoff)) != 0)[0]
        if crosses.size:
            be_low = float(S[crosses[0]])
            be_high = float(S[crosses[-1] + 1])
        else:
            be_low = be_high = float("nan")
        prob_profit = float((payoff > 0).sum() / len(payoff) * 100)

        # 5️⃣ Fetch the freshest greeks per leg
        greek_sql = """
          SELECT direction,
                 delta  * CASE WHEN direction='long' THEN 1 ELSE -1 END AS signed_delta,
                 theta  * CASE WHEN direction='long' THEN 1 ELSE -1 END AS signed_theta
          FROM (
            SELECT
              tl.direction,
              os.delta,
              os.theta,
              ROW_NUMBER() OVER (
                PARTITION BY CAST(os.strike AS STRING), os.option_type
                ORDER BY os.timestamp DESC
              ) AS rn
            FROM `{TL}` AS tl
            JOIN `{OS}` AS os
              ON tl.strike = os.strike
             AND tl.leg_type = os.option_type
            WHERE tl.trade_id = @tid
          )
          WHERE rn = 1
        """.format(
            TL=TRADE_LEGS, OS=OPT_SNAP
        )
        job_conf = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("tid", "STRING", trade_id)]
        )
        greeks = CLIENT.query(greek_sql, job_config=job_conf).to_dataframe()
        tot_delta = float(greeks["signed_delta"].sum())
        tot_theta = float(greeks["signed_theta"].sum())

        # 6️⃣ Insert analysis
        now_iso = datetime.now(timezone.utc).isoformat()
        row = {
            "trade_id": trade_id,
            "timestamp": now_iso,
            "max_profit": max_profit,
            "max_loss": max_loss,
            "breakeven_lower": be_low,
            "breakeven_upper": be_high,
            "probability_profit": prob_profit,
            "delta": tot_delta,
            "theta": tot_theta,
            "notes": "Auto‑computed via payoff grid",
        }
        errors = CLIENT.insert_rows_json(TRADE_PL_ANALYSIS, [row])
        if errors:
            logging.error("❌ P/L analysis insert errors: %s", errors)
            return

        logging.info("✅ Stored P/L analysis for %s", trade_id)

    except Exception:
        logging.exception("❌ Failed to compute P/L analysis for %s", trade_id)
