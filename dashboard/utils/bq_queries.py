# =====================
# utils/bq_queries.py
# BigQuery utility functions for Dash gamma dashboard and trade monitoring
# =====================

import logging
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from plotly.graph_objects import Figure, Surface

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT

TABLE_ID = f"{GOOGLE_CLOUD_PROJECT}.analytics.gamma_exposure"
TRADE_RECOMMENDATIONS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
LIVE_TRADE_PNL_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.live_trade_pnl"
TRADE_PL_ANALYSIS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_pl_analysis"
TRADE_PL_PROJECTIONS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_pl_projections"
TRADE_LEGS_TABLE = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"

# Create BigQuery client using proper credentials
CREDENTIALS = get_gcp_credentials()
CLIENT = bigquery.Client(credentials=CREDENTIALS, project=GOOGLE_CLOUD_PROJECT)


def get_available_expirations() -> List[str]:
    """
    Returns a list of upcoming expiration dates (today or later)
    available in the gamma_exposure table, formatted as YYYY‑MM‑DD.
    """
    query = f"""
    SELECT
      expiration_date
    FROM `{TABLE_ID}`
    WHERE expiration_date >= CURRENT_DATE()
    GROUP BY expiration_date
    ORDER BY expiration_date DESC
    LIMIT 30
    """
    try:
        df = CLIENT.query(query).to_dataframe()

        # make sure it's a datetime and format as string
        df["expiration_date"] = pd.to_datetime(df["expiration_date"], errors="coerce")
        df = df.dropna(subset=["expiration_date"])
        return df["expiration_date"].dt.strftime("%Y-%m-%d").tolist()

    except Exception as e:
        logging.error(f"Error fetching available expirations: {e}")
        return []


def get_trade_recommendations(status: str) -> pd.DataFrame:
    """
    Fetch trade recommendations based on status ('pending', 'active', 'closed').
    """
    query = f"""
        SELECT trade_id, strategy_type, symbol, entry_time, exit_time, 
               expiration_date, entry_price, exit_price, pnl, status
        FROM `{TRADE_RECOMMENDATIONS_TABLE}`
        WHERE status = @status
        ORDER BY entry_time DESC
        LIMIT 50
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("status", "STRING", status)]
    )

    try:
        return CLIENT.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        logging.error(f"Error fetching trade recommendations: {e}")
        return pd.DataFrame()


def get_legs_data(trade_id: str):
    """
    Fetch all legs associated with a trade_id from the trade_legs table.

    Args:
        trade_id (str): The unique identifier of the trade.

    Returns:
        pd.DataFrame: Dataframe containing leg_id, strike, direction, leg_type, entry_price, and status.
    """
    query = f"""
        SELECT leg_id, strike, direction, leg_type, entry_price, exit_price, pnl, status
        FROM `{TRADE_LEGS_TABLE}`
        WHERE trade_id = @trade_id
        ORDER BY strike ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("trade_id", "STRING", trade_id)]
    )

    try:
        legs_df = CLIENT.query(query, job_config=job_config).to_dataframe()
        if legs_df.empty:
            return pd.DataFrame(
                columns=["leg_id", "strike", "direction", "leg_type", "entry_price", "status"]
            )

        return legs_df

    except Exception as e:
        print(f"Error fetching legs for trade_id {trade_id}: {e}")
        return pd.DataFrame(
            columns=["leg_id", "strike", "direction", "leg_type", "entry_price", "status"]
        )


def get_live_pnl_data(trade_id: Optional[str] = None) -> pd.DataFrame:
    """
    If trade_id is None → return empty DataFrame.
    Otherwise return only the latest snapshot per leg for that trade.
    """
    if not trade_id:
        return pd.DataFrame()

    sql = f"""
    WITH latest AS (
      SELECT
        trade_id,
        leg_id,
        MAX(timestamp) AS ts
      FROM `{LIVE_TRADE_PNL_TABLE}`
      WHERE trade_id = @tid
      GROUP BY trade_id, leg_id
    )
    SELECT
      p.leg_id,
      p.current_price,
      p.theoretical_pnl
    FROM `{LIVE_TRADE_PNL_TABLE}` AS p
    JOIN latest AS l
      ON p.trade_id = l.trade_id
     AND p.leg_id   = l.leg_id
     AND p.timestamp= l.ts
    WHERE p.trade_id = @tid
    """
    job_conf = QueryJobConfig(query_parameters=[ScalarQueryParameter("tid", "STRING", trade_id)])

    try:
        return CLIENT.query(sql, job_config=job_conf).to_dataframe()
    except Exception as e:
        logging.error(f"get_live_pnl_data({trade_id}) failed: {e}")
        return pd.DataFrame()


def get_trade_pl_analysis(trade_id: str) -> pd.DataFrame:
    """
    Fetch P/L analysis data for a specific trade.
    """
    query = f"""
        SELECT max_profit, max_loss, breakeven_lower, breakeven_upper, 
               probability_profit, delta, theta, notes
        FROM `{TRADE_PL_ANALYSIS_TABLE}`
        WHERE trade_id = @trade_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("trade_id", "STRING", trade_id)]
    )

    try:
        return CLIENT.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        logging.error(f"Error fetching P/L analysis for trade {trade_id}: {e}")
        return pd.DataFrame()


def get_trade_pl_projections(trade_id: str) -> pd.DataFrame:
    """
    Fetch P/L projections for a specific trade over time.
    """
    query = f"""
        SELECT timestamp, underlying_price, pnl, delta, theta, gamma, vega, rho
        FROM `{TRADE_PL_PROJECTIONS_TABLE}`
        WHERE trade_id = @trade_id
        ORDER BY timestamp ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("trade_id", "STRING", trade_id)]
    )

    try:
        return CLIENT.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        logging.error(f"Error fetching P/L projections for trade {trade_id}: {e}")
        return pd.DataFrame()


def get_gamma_exposure_for_expiry(expiration_date: str) -> Tuple[pd.DataFrame, Optional[float]]:
    """
    Retrieves net gamma exposure by strike for a specific expiration date.
    """
    query = f"""
        SELECT strike, net_gamma_exposure, underlying_price
        FROM `{TABLE_ID}`
        WHERE expiration_date = @expiration_date
        AND timestamp = (
            SELECT MAX(timestamp)
            FROM `{TABLE_ID}`
            WHERE expiration_date = @expiration_date
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("expiration_date", "STRING", expiration_date)
        ]
    )

    try:
        df = CLIENT.query(query, job_config=job_config).to_dataframe()
        if df.empty:
            return pd.DataFrame(), None

        grouped = df.groupby("strike")["net_gamma_exposure"].sum().reset_index()
        current_price = df["underlying_price"].dropna().median()

        return grouped, current_price
    except Exception as e:
        logging.error(f"Error fetching gamma exposure for expiry {expiration_date}: {e}")
        return pd.DataFrame(), None


def get_gamma_exposure_surface_data(
    start_date: Optional[str] = None, end_date: Optional[str] = None
) -> Figure:
    """
    Returns a 3D surface plot of Strike × Expiry × Gamma Exposure.

    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format.
        end_date (str, optional): End date in YYYY-MM-DD format.

    Returns:
        Figure: Plotly 3D Surface plot.
    """
    query = f"""
        SELECT expiration_date, strike, SUM(net_gamma_exposure) AS gex
        FROM `{TABLE_ID}`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
    """

    if start_date:
        query += f" AND expiration_date >= '{start_date}'"
    if end_date:
        query += f" AND expiration_date <= '{end_date}'"

    query += " GROUP BY expiration_date, strike"

    try:
        df = CLIENT.query(query).to_dataframe()
        if df.empty:
            return Figure(layout={"title": "No data for 3D Gamma Exposure surface."})

        df["expiration_date"] = pd.to_datetime(df["expiration_date"])
        pivot = df.pivot_table(
            index="strike", columns="expiration_date", values="gex", fill_value=0
        )

        z_raw = pivot.values
        z_clipped = np.clip(z_raw, -1e8, 1e8)
        x = pivot.index
        y = pivot.columns.strftime("%Y-%m-%d")

        fig = Figure(
            data=[
                Surface(
                    z=z_clipped,
                    x=x,
                    y=y,
                    colorscale="RdBu",
                    reversescale=True,
                    showscale=True,
                    opacity=0.95,
                    lighting=dict(ambient=0.6, diffuse=0.8),
                    lightposition=dict(x=0, y=0, z=300),
                )
            ]
        )

        fig.update_layout(
            title="3D Gamma Exposure Surface",
            scene=dict(
                xaxis_title="Strike Price",
                yaxis_title="Expiration Date",
                zaxis_title="Net Gamma Exposure",
            ),
            margin=dict(l=0, r=0, b=0, t=50),
            height=600,
            template="plotly_white",
        )

        return fig

    except Exception as e:
        logging.error(f"Error generating 3D Gamma Exposure surface plot: {e}")
        return Figure(layout={"title": "Error generating 3D Gamma Exposure surface."})


def get_trade_ids() -> List[str]:
    """
    Fetch distinct trade IDs for P/L Analysis dropdown.
    """
    query = f"""
        SELECT DISTINCT trade_id 
        FROM `{TRADE_RECOMMENDATIONS_TABLE}`
        ORDER BY trade_id DESC
        LIMIT 100
    """
    try:
        df = CLIENT.query(query).to_dataframe()
        return df["trade_id"].tolist()
    except Exception as e:
        logging.error(f"Error fetching trade IDs: {e}")
        return []
