# trade/pl_analysis.py
# =====================
# Compute and store P/L analysis (grid + analytic + Greeks) for a given trade_id.
# Supports passing in legs_data/spot_price/root_symbol to avoid redundant queries.
# =====================

import logging
import math
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import pytz
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT

# ── Table names ───────────────────────────────────────────────────────────────
TRADE_LEGS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
TRADE_RECS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
OPTION_SNAPSHOT_TABLE = f"{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot"
TRADE_PL_ANALYSIS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_pl_analysis"
INDEX_PRICE_TABLE = f"{GOOGLE_CLOUD_PROJECT}.market_data.index_price_snapshot"

# ── BigQuery client ───────────────────────────────────────────────────────────
CLIENT = bigquery.Client(
    credentials=get_gcp_credentials(),
    project=GOOGLE_CLOUD_PROJECT,
)


def norm_cdf(x: float) -> float:
    """
    Standard normal CDF: Φ(x) = 0.5 * [1 + erf(x/√2)].
    """
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def compute_and_store_pl_analysis(
    trade_id: str,
    legs_data: Optional[pd.DataFrame] = None,
    spot_price: Optional[float] = None,
    root_symbol: Optional[str] = None,
    grid_points: int = 200,
    underlying_range: float = 0.2,
):
    """
    1) Load legs (from DataFrame or BQ).
    2) Fetch symbol & expiration_date from trade_recommendations.
    3) Determine spot (passed-in or latest index_price_snapshot).
    4) Build a payoff grid around spot and compute max_profit, max_loss, breakevens.
    5) Analytically compute probability_of_profit under log-normal assumption.
    6) Fetch signed Greeks (delta, theta) for each leg.
    7) Insert a summary row into trade_pl_analysis.
    """
    logging.info("🔍 Computing P/L analysis for trade %s", trade_id)

    # 1️⃣ Load trade legs
    if legs_data is not None:
        legs_df = legs_data.copy()
    else:
        sql = f"""
            SELECT leg_type, direction, strike, entry_price
            FROM `{TRADE_LEGS_TABLE}`
            WHERE trade_id = @tid
        """
        legs_df = CLIENT.query(
            sql,
            job_config=QueryJobConfig(
                query_parameters=[ScalarQueryParameter("tid", "STRING", trade_id)]
            ),
        ).to_dataframe()
        if legs_df.empty:
            logging.warning("No legs found for trade %s; skipping P/L analysis.", trade_id)
            return

    # 2️⃣ Fetch trade metadata (symbol & expiration_date)
    meta_sql = f"""
        SELECT symbol, expiration_date
        FROM `{TRADE_RECS_TABLE}`
        WHERE trade_id = @tid
    """
    meta = (
        CLIENT.query(
            meta_sql,
            job_config=QueryJobConfig(
                query_parameters=[ScalarQueryParameter("tid", "STRING", trade_id)]
            ),
        )
        .to_dataframe()
        .iloc[0]
    )
    symbol = meta["symbol"]
    exp_date = meta["expiration_date"]
    # Decide which root_symbol to use for option lookups (e.g. 'SPXW', 'QQQW', etc.)
    root_sym = root_symbol or f"{symbol}W"

    # 3️⃣ Determine spot price
    if spot_price is not None:
        spot = spot_price
    else:
        spot_sql = f"""
            SELECT last AS spot
            FROM `{INDEX_PRICE_TABLE}`
            WHERE symbol = @sym
            ORDER BY timestamp DESC
            LIMIT 1
        """
        spot = float(
            CLIENT.query(
                spot_sql,
                job_config=QueryJobConfig(
                    query_parameters=[ScalarQueryParameter("sym", "STRING", symbol)]
                ),
            )
            .to_dataframe()["spot"]
            .iloc[0]
        )

    # 4️⃣ Build payoff grid and extract P/L metrics
    low, high = spot * (1 - underlying_range), spot * (1 + underlying_range)
    prices = np.linspace(low, high, grid_points)
    payoff = np.zeros_like(prices)
    for _, leg in legs_df.iterrows():
        K, ent = leg["strike"], leg["entry_price"]
        sign = 1 if leg["direction"] == "long" else -1
        intrinsic = (
            np.maximum(prices - K, 0) if leg["leg_type"] == "call" else np.maximum(K - prices, 0)
        )
        payoff += sign * intrinsic - sign * ent
    payoff *= 100  # contract multiplier

    max_profit = float(payoff.max())
    max_loss = float(payoff.min())
    crosses = np.where(np.diff(np.sign(payoff)) != 0)[0]
    if crosses.size:
        be_low = float(prices[crosses[0]])
        be_high = float(prices[crosses[-1] + 1])
    else:
        be_low = be_high = None

    # 5️⃣ Analytic probability of profit
    now_utc = datetime.now(timezone.utc)
    exp_dt = datetime(exp_date.year, exp_date.month, exp_date.day, 16, 0, 0)
    exp_et = pytz.timezone("America/New_York").localize(exp_dt)
    exp_utc = exp_et.astimezone(timezone.utc)
    T_years = max((exp_utc - now_utc).total_seconds(), 0) / (365 * 24 * 3600)

    # Identify the two short strikes
    put_strikes = legs_df.query("leg_type=='put'   and direction=='short'")["strike"].tolist()
    call_strikes = legs_df.query("leg_type=='call'  and direction=='short'")["strike"].tolist()
    if put_strikes and call_strikes and T_years > 0:
        Kp, Kc = put_strikes[0], call_strikes[0]
        iv_sql = f"""
            SELECT mid_iv
            FROM `{OPTION_SNAPSHOT_TABLE}` AS os
            WHERE os.root_symbol     = @rsym
              AND os.expiration_date = @exp_date
              AND ((os.option_type='put'  AND os.strike = @Kp)
                OR (os.option_type='call' AND os.strike = @Kc))
            ORDER BY os.timestamp DESC
            LIMIT 2
        """
        iv_df = CLIENT.query(
            iv_sql,
            job_config=QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("rsym", "STRING", root_sym),
                    ScalarQueryParameter("exp_date", "DATE", exp_date),
                    ScalarQueryParameter("Kp", "FLOAT64", Kp),
                    ScalarQueryParameter("Kc", "FLOAT64", Kc),
                ]
            ),
        ).to_dataframe()

        if not iv_df.empty:
            sigma = float(iv_df["mid_iv"].mean())
            if sigma > 0:
                d2_put = (math.log(spot / Kp) - 0.5 * sigma**2 * T_years) / (
                    sigma * math.sqrt(T_years)
                )
                d2_call = (math.log(spot / Kc) - 0.5 * sigma**2 * T_years) / (
                    sigma * math.sqrt(T_years)
                )
                prob_profit = norm_cdf(d2_put) - norm_cdf(d2_call)
            else:
                prob_profit = None
        else:
            prob_profit = None
    else:
        prob_profit = None

    # 6️⃣ Fetch signed Greeks (delta, theta) for each leg
    greek_sql = f"""
        SELECT signed_delta, signed_theta
        FROM (
          SELECT tl.direction,
                 os.delta * CASE WHEN tl.direction='long' THEN 1 ELSE -1 END   AS signed_delta,
                 os.theta * CASE WHEN tl.direction='long' THEN 1 ELSE -1 END   AS signed_theta,
                 ROW_NUMBER() OVER (
                   PARTITION BY CAST(os.strike AS STRING), os.option_type
                   ORDER BY os.timestamp DESC
                 ) AS rn
          FROM `{TRADE_LEGS_TABLE}` AS tl
          JOIN `{OPTION_SNAPSHOT_TABLE}` AS os
            ON tl.strike     = os.strike
           AND tl.leg_type   = os.option_type
          WHERE tl.trade_id       = @tid
            AND os.root_symbol    = @rsym
            AND os.expiration_date= @exp_date
        ) AS numbered
        WHERE rn = 1
    """
    greek_df = CLIENT.query(
        greek_sql,
        job_config=QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("tid", "STRING", trade_id),
                ScalarQueryParameter("rsym", "STRING", root_sym),
                ScalarQueryParameter("exp_date", "DATE", exp_date),
            ]
        ),
    ).to_dataframe()
    tot_delta = float(greek_df["signed_delta"].sum()) if not greek_df.empty else 0.0
    tot_theta = float(greek_df["signed_theta"].sum()) if not greek_df.empty else 0.0

    # 7️⃣ Insert summary into trade_pl_analysis
    analysis_row = {
        "trade_id": trade_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "max_profit": max_profit,
        "max_loss": max_loss,
        "breakeven_lower": be_low,
        "breakeven_upper": be_high,
        "probability_profit": None if prob_profit is None else float(prob_profit * 100),
        "delta": tot_delta,
        "theta": tot_theta,
        "notes": "P/L grid + analytic prob under log-normal",
    }
    CLIENT.insert_rows_json(TRADE_PL_ANALYSIS_TABLE, [analysis_row])
    logging.info("✅ Stored P/L analysis for %s", trade_id)
