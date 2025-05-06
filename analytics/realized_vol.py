# =====================
# analytics/realized_vol.py
# Computes short-term realized volatility from index prices
# =====================
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from pandas_gbq import to_gbq

from common.auth import get_gcp_credentials
from common.config import GOOGLE_CLOUD_PROJECT
from common.utils import is_trading_hours


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
            group = group.sort_values("timestamp")
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
