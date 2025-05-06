# =====================
# utils/bq_queries.py
# BigQuery utility functions for Dash gamma dashboard
# =====================

import logging
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from google.cloud import bigquery
from plotly.graph_objects import Figure, Surface

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


def get_gamma_exposure_surface_data() -> Figure:
    """
    Returns a 3D surface plot of Strike × Expiry × Gamma Exposure
    """
    query = f"""
        SELECT expiration_date, strike, SUM(net_gamma_exposure) AS gex
        FROM `{TABLE_ID}`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
        GROUP BY expiration_date, strike
    """
    df = CLIENT.query(query).to_dataframe()
    if df.empty:
        return Figure(layout={"title": "No data for 3D Gamma Exposure surface."})

    df["expiration_date"] = pd.to_datetime(df["expiration_date"])
    pivot = df.pivot_table(index="strike", columns="expiration_date", values="gex", fill_value=0)

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
