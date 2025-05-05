# =====================
# utils/bq_queries.py
# BigQuery utility functions for Dash gamma dashboard
# =====================

import os

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from common.auth import get_gcp_credentials

# Load .env only in local development
if not (os.getenv("RENDER") or os.getenv("RAILWAY_ENVIRONMENT")):
    from pathlib import Path

    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

# Project and table config
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
TABLE_ID = f"{PROJECT_ID}.analytics.gamma_exposure"

# Create BigQuery client using proper credentials
CREDENTIALS = get_gcp_credentials()
CLIENT = bigquery.Client(credentials=CREDENTIALS, project=PROJECT_ID)


def get_available_expirations():
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
    return df["expiration_date"].dt.strftime("%Y-%m-%d").tolist()


def get_gamma_exposure_for_expiry(expiration_date: str):
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
    current_price = df["underlying_price"].dropna().median()

    return grouped, current_price
