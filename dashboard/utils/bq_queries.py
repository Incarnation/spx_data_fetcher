# =====================
# utils/bq_queries.py
# BigQuery utility functions for Dash gamma dashboard
# =====================

import logging
from typing import List, Optional, Tuple

import pandas as pd
from google.cloud import bigquery
from plotly.graph_objects import Figure, Scatter

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT

TABLE_ID = f"{GOOGLE_CLOUD_PROJECT}.analytics.gamma_exposure"

# Create BigQuery client using proper credentials
CREDENTIALS = get_gcp_credentials()
CLIENT = bigquery.Client(credentials=CREDENTIALS, project=GOOGLE_CLOUD_PROJECT)


def get_available_expirations() -> List[str]:
    """
    Returns a list of recent expiration dates available in the gamma exposure table.
    Returns:
        List[str]: Dates in YYYY-MM-DD format.
    """
    query = f"""
        SELECT DISTINCT expiration_date
        FROM `{TABLE_ID}`
        ORDER BY expiration_date DESC
        LIMIT 30
    """
    df = CLIENT.query(query).to_dataframe()

    # 👇 Ensure expiration_date is a datetime column
    df["expiration_date"] = pd.to_datetime(df["expiration_date"])

    return df["expiration_date"].dt.strftime("%Y-%m-%d").tolist()


def get_gamma_exposure_for_expiry(expiration_date: str) -> Tuple[pd.DataFrame, Optional[float]]:
    """
    Retrieves net gamma exposure by strike for a specific expiration date.

    Args:
        expiration_date (str): The target expiration date (format: YYYY-MM-DD).

    Returns:
        Tuple[pd.DataFrame, float]: Gamma exposure DataFrame and current spot price.
    """
    query = f"""
        SELECT strike, net_gamma_exposure, underlying_price
        FROM `{TABLE_ID}`
        WHERE expiration_date = "{expiration_date}"
        AND timestamp = (
            SELECT MAX(timestamp)
            FROM `{TABLE_ID}`
            WHERE expiration_date = "{expiration_date}"
        )
    """
    df = CLIENT.query(query).to_dataframe()

    if df.empty:
        return pd.DataFrame(), None

    grouped = df.groupby("strike")["net_gamma_exposure"].sum().reset_index()
    current_price_series = df["underlying_price"].dropna()
    current_price = current_price_series.median() if not current_price_series.empty else None

    if current_price is None:
        logging.warning(f"No underlying price available for {expiration_date}")

    return grouped, current_price


def get_realized_volatility():
    """
    Returns a Plotly figure for most recent 1H and 1D realized vol for all symbols.
    """
    query = f"""
        SELECT *
        FROM `{GOOGLE_CLOUD_PROJECT}.analytics.realized_volatility`
        WHERE timestamp = (
            SELECT MAX(timestamp)
            FROM `{GOOGLE_CLOUD_PROJECT}.analytics.realized_volatility`
        )
    """
    df = CLIENT.query(query).to_dataframe()
    if df.empty:
        return Figure(layout={"title": "No realized volatility data found."})

    fig = Figure()
    fig.add_trace(Scatter(x=df["symbol"], y=df["vol_1h"], name="1H Vol", mode="lines+markers"))
    fig.add_trace(Scatter(x=df["symbol"], y=df["vol_1d"], name="1D Vol", mode="lines+markers"))

    fig.update_layout(
        title="Realized Volatility (Most Recent)",
        xaxis_title="Symbol",
        yaxis_title="Volatility (Annualized)",
        template="plotly_white",
        height=400,
    )
    return fig
