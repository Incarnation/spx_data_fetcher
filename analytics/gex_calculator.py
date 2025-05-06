# analytics/gex_calculator.py
import logging

from google.cloud import bigquery
from pandas_gbq import to_gbq

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT
from common.utils import is_trading_hours


def calculate_and_store_gex():
    try:
        if not is_trading_hours():
            logging.info("⏳ Market closed, skipping calculate_and_store_gex.")
            return

        credentials = get_gcp_credentials()
        client = bigquery.Client(credentials=credentials, project=GOOGLE_CLOUD_PROJECT)

        query = f"""
        WITH latest AS (
            SELECT symbol, expiration_date, MAX(timestamp) AS ts
            FROM `{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot`
            GROUP BY symbol, expiration_date
        )
        SELECT
            a.symbol,
            a.expiration_date,
            a.strike,
            a.timestamp,
            a.underlying_price,
            SUM(CASE WHEN a.option_type = 'put' THEN -1 ELSE 1 END * a.gamma * a.open_interest * 100) AS net_gamma_exposure
        FROM `{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot` a
        JOIN latest b ON a.symbol = b.symbol AND a.expiration_date = b.expiration_date AND a.timestamp = b.ts
        GROUP BY a.symbol, a.expiration_date, a.strike, a.timestamp, a.underlying_price
        """

        df = client.query(query).to_dataframe()
        if df.empty:
            logging.warning("⚠️ No gamma exposure data calculated. Skipping upload.")
            return

        logging.info(f"✅ Calculated and uploading {len(df)} GEX rows to analytics.gamma_exposure")
        table_id = f"{GOOGLE_CLOUD_PROJECT}.analytics.gamma_exposure"
        to_gbq(
            df,
            table_id,
            project_id=GOOGLE_CLOUD_PROJECT,
            if_exists="append",
            credentials=credentials,
        )
    except Exception as e:
        logging.exception(f"💥 Error in calculate_and_store_gex: {e}")
