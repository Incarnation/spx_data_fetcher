# =====================
# dashboard/utils.py
# Utility to query BigQuery and calculate net gamma exposure
# =====================
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")


def get_available_expirations():
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    table_id = os.getenv("OPTION_CHAINS_TABLE_ID")
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=project_id)

    query = f"""
    SELECT DISTINCT expiration_date
    FROM `{table_id}`
    ORDER BY expiration_date DESC
    LIMIT 30
    """
    df = client.query(query).to_dataframe()
    df["expiration_date"] = pd.to_datetime(df["expiration_date"])
    return df["expiration_date"].dt.strftime("%Y-%m-%d").tolist()


def get_gamma_exposure_df(expiration):
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    table_id = os.getenv("OPTION_CHAINS_TABLE_ID")
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=project_id)

    query = f"""
    SELECT
        strike,
        option_type,
        gamma,
        open_interest,
        underlying_price,
        (CASE WHEN option_type = 'put' THEN -1 ELSE 1 END) * gamma * open_interest * 100 AS gamma_exposure
    FROM `{table_id}`
    WHERE expiration_date = "{expiration}"
    AND timestamp = (
        SELECT MAX(timestamp) FROM `{table_id}`
        WHERE expiration_date = "{expiration}"
    )
    """

    df = client.query(query).to_dataframe()
    if df.empty:
        return pd.DataFrame(), None

    gex_by_strike = df.groupby("strike")["gamma_exposure"].sum().reset_index()
    current_price = df["underlying_price"].dropna().median()
    return gex_by_strike, current_price
