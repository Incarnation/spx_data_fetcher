import logging

from google.cloud import bigquery
from pandas_gbq import to_gbq

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT
from common.utils import is_trading_hours


def calculate_and_store_gex():
    """
    Calculates net gamma exposure (GEX) for each symbol, expiration_date, and strike,
    then appends only new snapshots into analytics.gamma_exposure. Ensures idempotency by
    processing only data newer than the last stored timestamp, and runs only during trading hours.

    Columns loaded:
      - symbol (e.g. 'SPX')
      - expiration_date (DATE)
      - strike (FLOAT)
      - timestamp (TIMESTAMP of snapshot)
      - underlying_price (FLOAT)
      - net_gamma_exposure (FLOAT: sum of gamma * open_interest * 100, puts negative)
    """
    try:
        # 1) Skip processing outside market hours
        if not is_trading_hours():
            logging.info("⏳ Market closed, skipping calculate_and_store_gex.")
            return

        # 2) Initialize BigQuery client
        credentials = get_gcp_credentials()
        client = bigquery.Client(credentials=credentials, project=GOOGLE_CLOUD_PROJECT)

        # 3) Determine last processed timestamp to avoid duplicates
        max_ts_sql = f"""
        SELECT
          MAX(timestamp) AS last_ts
        FROM `{GOOGLE_CLOUD_PROJECT}.analytics.gamma_exposure`
        """
        last_ts_df = client.query(max_ts_sql).to_dataframe()
        last_ts = last_ts_df.loc[0, "last_ts"] if not last_ts_df.empty else None

        # Build a filter clause if we have already processed data
        snapshot_filter = ""
        if last_ts is not None:
            # Only include snapshots after the last processed timestamp
            snapshot_filter = f"WHERE a.timestamp > TIMESTAMP('{last_ts.isoformat()}')"

        # 4) Calculate net gamma exposure from the latest option_chain_snapshot per symbol/expiry
        query = f"""
        WITH latest AS (
          SELECT
            a.symbol,
            a.expiration_date,
            MAX(a.timestamp) AS ts
          FROM `{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot` a
          {snapshot_filter}
          GROUP BY a.symbol, a.expiration_date
        )
        SELECT
          a.symbol,
          a.expiration_date,
          a.strike,
          l.ts          AS timestamp,
          a.underlying_price,
          SUM(
            CASE WHEN a.option_type = 'put' THEN -1 ELSE 1 END
            * a.gamma
            * a.open_interest
            * 100
          ) AS net_gamma_exposure
        FROM `{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot` a
        JOIN latest l
          ON a.symbol = l.symbol
         AND a.expiration_date = l.expiration_date
         AND a.timestamp = l.ts
        GROUP BY
          a.symbol, 
          a.expiration_date, 
          a.strike, 
          l.ts, 
          a.underlying_price
        """

        df = client.query(query).to_dataframe()

        # 5) If no new rows, skip upload
        if df.empty:
            logging.info("⚠️ No new GEX data to upload.")
            return

        # 6) Append to analytics.gamma_exposure
        table_id = f"{GOOGLE_CLOUD_PROJECT}.analytics.gamma_exposure"
        logging.info(f"✅ Uploading {len(df)} new GEX rows to {table_id}")
        to_gbq(
            df,
            table_id,
            project_id=GOOGLE_CLOUD_PROJECT,
            if_exists="append",
            credentials=credentials,
        )

    except Exception as e:
        logging.exception(f"💥 Error in calculate_and_store_gex: {e}")
