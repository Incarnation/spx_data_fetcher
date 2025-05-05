# analytics/gex_calculator.py
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery
from pandas_gbq import to_gbq

from common.auth import get_gcp_credentials
from common.utils import is_trading_hours

# Load .env only if running locally
if not (os.getenv("RENDER") or os.getenv("RAILWAY_ENVIRONMENT")):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")


def calculate_and_store_gex():
    if not is_trading_hours():
        logging.info("⏳ Market closed, skipping calculate_and_store_gex.")
        return

    credentials = get_gcp_credentials()
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    query = f"""
    WITH latest AS (
        SELECT symbol, expiration_date, MAX(timestamp) AS ts
        FROM `{PROJECT_ID}.options.option_chain_snapshot`
        GROUP BY symbol, expiration_date
    )
    SELECT
        a.symbol,
        a.expiration_date,
        a.strike,
        a.timestamp,
        a.underlying_price,
        SUM(CASE WHEN a.option_type = 'put' THEN -1 ELSE 1 END * a.gamma * a.open_interest * 100) AS net_gamma_exposure
    FROM `{PROJECT_ID}.options.option_chain_snapshot` a
    JOIN latest b ON a.symbol = b.symbol AND a.expiration_date = b.expiration_date AND a.timestamp = b.ts
    GROUP BY a.symbol, a.expiration_date, a.strike, a.timestamp, a.underlying_price
    """

    df = client.query(query).to_dataframe()
    if df.empty:
        return

    table_id = f"{PROJECT_ID}.analytics.gamma_exposure"
    to_gbq(df, table_id, project_id=PROJECT_ID, if_exists="append", credentials=credentials)
