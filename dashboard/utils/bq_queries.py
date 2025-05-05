# =====================
# utils/bq_queries.py
# BigQuery utility functions for Dash gamma dashboard
# =====================
import json
import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Load .env if running locally
if not (os.getenv("RENDER") or os.getenv("RAILWAY_ENVIRONMENT")):
    from pathlib import Path

    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

# Load project ID and credentials
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

# Authenticate with Google Cloud
if not SERVICE_ACCOUNT_JSON:
    raise EnvironmentError("Missing GOOGLE_SERVICE_ACCOUNT_JSON environment variable")


credentials_info = service_account.Credentials.from_service_account_info(
    json.loads(SERVICE_ACCOUNT_JSON)
)


CLIENT = bigquery.Client(credentials=credentials_info, project=PROJECT_ID)

# Name of the gamma exposure table
TABLE_ID = f"{PROJECT_ID}.analytics.gamma_exposure"


def get_available_expirations():
    """
    Fetch distinct expiration dates from the gamma exposure table.
    Sorted in descending order (most recent first).
    """
    query = f"""
        SELECT DISTINCT expiration_date
        FROM `{TABLE_ID}`
        ORDER BY expiration_date DESC
        LIMIT 30
    """
    df = CLIENT.query(query).to_dataframe()
    return df["expiration_date"].dt.strftime("%Y-%m-%d").tolist()


def get_gamma_exposure_df(expiration_date: str):
    """
    Fetch gamma exposure values for a given expiration date.

    Parameters:
        expiration_date (str): The expiration date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: Gamma exposure aggregated by strike.
        float: Median underlying price for the snapshot.
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

    # Group net gamma exposure by strike
    gex_by_strike = df.groupby("strike")["net_gamma_exposure"].sum().reset_index()
    # Use median price as the most stable underlying reference
    current_price = df["underlying_price"].dropna().median()
    return gex_by_strike, current_price
