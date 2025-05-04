# =====================
# app/uploader.py
# Upload options and index price to BigQuery (with explicit credentials)
# =====================
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from pandas_gbq import to_gbq

# Load .env only if running locally (optional guard)
if os.getenv("RENDER") is None:
    from pathlib import Path

    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
option_table_id = os.getenv("OPTION_CHAINS_TABLE_ID")
index_price_table_id = os.getenv("INDEX_PRICE_TABLE_ID")
credentials = service_account.Credentials.from_service_account_file(credentials_path)


def upload_to_bigquery(options, timestamp, expiration, underlying_price=None):
    rows = []
    for opt in options:
        g = opt.get("greeks", {})
        rows.append(
            {
                "timestamp": timestamp,
                "symbol": opt.get("symbol"),
                "root_symbol": opt.get("root_symbol"),
                "option_type": opt.get("option_type"),
                "expiration_date": opt.get("expiration_date"),
                "expiration_type": opt.get("expiration_type"),
                "strike": opt.get("strike"),
                "bid": opt.get("bid"),
                "ask": opt.get("ask"),
                "last": opt.get("last"),
                "change": opt.get("change"),
                "change_percentage": opt.get("change_percentage"),
                "volume": opt.get("volume"),
                "open_interest": opt.get("open_interest"),
                "bidsize": opt.get("bidsize"),
                "asksize": opt.get("asksize"),
                "high": opt.get("high"),
                "low": opt.get("low"),
                "open": opt.get("open"),
                "close": opt.get("close"),
                "delta": g.get("delta"),
                "gamma": g.get("gamma"),
                "theta": g.get("theta"),
                "vega": g.get("vega"),
                "rho": g.get("rho"),
                "bid_iv": g.get("bid_iv"),
                "ask_iv": g.get("ask_iv"),
                "mid_iv": g.get("mid_iv"),
                "smv_vol": g.get("smv_vol"),
                "underlying_price": underlying_price.get("last"),
            }
        )

    df = pd.DataFrame(rows)

    try:
        to_gbq(
            df, option_table_id, project_id=project_id, if_exists="append", credentials=credentials
        )
        logging.info(f"✅ Uploaded {len(df)} rows for {expiration}")
    except Exception as e:
        logging.error(f"❌ Failed to upload data to BigQuery: {e}")


def upload_index_price(symbol: str, quote: dict):
    if not quote or "last" not in quote:
        logging.warning(f"⚠️ Invalid quote for {symbol}")
        return

    now = datetime.utcnow()
    df = pd.DataFrame(
        [
            {
                "timestamp": now,
                "symbol": symbol,
                "last": quote.get("last"),
                "high": quote.get("high"),
                "low": quote.get("low"),
                "open": quote.get("open"),
                "close": quote.get("close"),
                "volume": quote.get("volume"),
            }
        ]
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    to_gbq(
        df, index_price_table_id, project_id=project_id, if_exists="append", credentials=credentials
    )
