# =====================
# dashboard/utils.py
# Utility to query BigQuery and calculate net gamma exposure
# =====================
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from common.auth import get_gcp_credentials

# Load .env only if running locally (optional guard)
if not (os.getenv("RENDER") or os.getenv("RAILWAY_ENVIRONMENT")):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
TABLE_ID = os.getenv("OPTION_CHAINS_TABLE_ID")


def get_bigquery_client():
    credentials = get_gcp_credentials()
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


def get_available_expirations():
    client = get_bigquery_client()
    query = f"""
    SELECT DISTINCT expiration_date
    FROM `{TABLE_ID}`
    ORDER BY expiration_date DESC
    LIMIT 30
    """
    df = client.query(query).to_dataframe()
    df["expiration_date"] = pd.to_datetime(df["expiration_date"])
    return df["expiration_date"].dt.strftime("%Y-%m-%d").tolist()


def get_gamma_exposure_df(expiration):
    client = get_bigquery_client()
    query = f"""
    SELECT
        strike,
        option_type,
        gamma,
        open_interest,
        underlying_price,
        (CASE WHEN option_type = 'put' THEN -1 ELSE 1 END) * gamma * open_interest * 100 AS gamma_exposure
    FROM `{TABLE_ID}`
    WHERE expiration_date = "{expiration}"
    AND timestamp = (
        SELECT MAX(timestamp) FROM `{TABLE_ID}`
        WHERE expiration_date = "{expiration}"
    )
    """
    df = client.query(query).to_dataframe()
    if df.empty:
        return pd.DataFrame(), None

    gex_by_strike = df.groupby("strike")["gamma_exposure"].sum().reset_index()
    current_price = df["underlying_price"].dropna().median()
    return gex_by_strike, current_price
