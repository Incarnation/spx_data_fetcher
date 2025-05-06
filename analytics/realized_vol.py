# =====================
# analytics/realized_vol.py
# Computes short-term realized volatility from index prices
# =====================

import logging

import numpy as np
import pandas as pd
from google.cloud import bigquery
from pandas_gbq import to_gbq

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT
from common.utils import is_trading_hours


def is_us_trading_hour(ts_utc):
    """
    Returns True if the UTC timestamp falls between 9:30am–4:00pm Eastern Time.
    """
    ts_est = ts_utc.tz_convert("America/New_York")
    return ts_est.weekday() < 5 and (
        ts_est.time() >= pd.Timestamp("09:30").time()
        and ts_est.time() <= pd.Timestamp("16:00").time()
    )


def calculate_and_store_realized_vol():
    try:
        if not is_trading_hours():
            logging.info("⏳ Market closed, skipping calculate_and_store_realized_vol.")
            return

        credentials = get_gcp_credentials()
        client = bigquery.Client(credentials=credentials, project=GOOGLE_CLOUD_PROJECT)

        query = f"""
        SELECT *
        FROM `{GOOGLE_CLOUD_PROJECT}.market_data.index_price_snapshot`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
        ORDER BY symbol, timestamp
        """
        df = client.query(query).to_dataframe()
        if df.empty:
            logging.warning("⚠️ No data found in index_price_snapshot. Exiting.")
            return

        results = []
        for symbol, group in df.groupby("symbol"):
            group["timestamp"] = pd.to_datetime(group["timestamp"], utc=True)
            group = group.sort_values("timestamp")

            # 👇 Filter to only U.S. trading hours (Eastern Time)
            group = group[group["timestamp"].apply(is_us_trading_hour)]

            group["log_return"] = np.log(group["last"] / group["last"].shift(1))

            # 1H realized volatility (5 min intervals → 12 samples/hour)
            group["vol_1h"] = group["log_return"].rolling(window=12).std() * np.sqrt(12)

            # 1D realized volatility (5 min intervals → ~78 per day)
            group["vol_1d"] = group["log_return"].rolling(window=78).std() * np.sqrt(78)

            clean = group.dropna(subset=["vol_1h", "vol_1d"])
            if clean.empty:
                logging.warning(f"⚠️ Not enough data to compute volatility for {symbol}")
                continue

            last_row = clean.iloc[-1]
            results.append(
                {
                    "timestamp": last_row["timestamp"],
                    "symbol": symbol,
                    "vol_1h": last_row["vol_1h"],
                    "vol_1d": last_row["vol_1d"],
                }
            )

        if results:
            vol_df = pd.DataFrame(results)
            vol_df["timestamp"] = pd.to_datetime(vol_df["timestamp"], utc=True)
            logging.info(f"📤 Uploading {len(vol_df)} realized volatility rows to BigQuery...")
            table_id = f"{GOOGLE_CLOUD_PROJECT}.analytics.realized_volatility"
            to_gbq(
                vol_df,
                table_id,
                project_id=GOOGLE_CLOUD_PROJECT,
                if_exists="append",
                credentials=credentials,
            )
        else:
            logging.info("📭 No volatility records to upload.")

    except Exception as e:
        logging.exception(f"💥 Error in calculate_and_store_realized_vol: {e}")
