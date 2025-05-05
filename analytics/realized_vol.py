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
from google.oauth2 import service_account
from pandas_gbq import to_gbq

# Load .env only if running locally (optional guard)
if os.getenv("RENDER") is None:
    from pathlib import Path

    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
CREDENTIALS = service_account.Credentials.from_service_account_file(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
)
CLIENT = bigquery.Client(credentials=CREDENTIALS, project=PROJECT_ID)


def calculate_and_store_realized_vol():

    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.market_data.index_price_snapshot`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
    ORDER BY symbol, timestamp
    """
    df = CLIENT.query(query).to_dataframe()
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

        # Drop rows with NaNs from rolling std
        clean = group.dropna(subset=["vol_1h", "vol_1d"])
        if clean.empty:
            logging.warning(f"⚠️ Not enough data to compute volatility for {symbol}")
            continue  # Not enough data

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
        logging.info(f"📤 Uploading {len(vol_df)} realized volatility rows to BigQuery...")
        table_id = f"{PROJECT_ID}.analytics.realized_volatility"
        to_gbq(vol_df, table_id, project_id=PROJECT_ID, if_exists="append", credentials=CREDENTIALS)
    else:
        logging.info("📭 No volatility records to upload.")
