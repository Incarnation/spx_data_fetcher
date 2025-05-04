# =====================
# app/uploader.py
# Uploads option chain data to BigQuery
# =====================
import os
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account


def upload_to_bigquery(options, timestamp, expiration, underlying_price=None):
    from dotenv import load_dotenv

    load_dotenv()
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    table_id = os.getenv("OPTION_CHAINS_TABLE_ID")
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path
    )

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
                "underlying_price": underlying_price,
            }
        )

    df = pd.DataFrame(rows)
    to_gbq(
        df, table_id, project_id=project_id, if_exists="append", credentials=credentials
    )
