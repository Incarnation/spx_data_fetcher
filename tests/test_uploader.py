# test_uploader.py

from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from app import uploader


@patch("app.uploader.to_gbq")
def test_upload_to_bigquery(mock_to_gbq):
    options = [
        {
            "symbol": "SPX240503P05000000",
            "root_symbol": "SPX",
            "option_type": "put",
            "expiration_date": "2024-05-03",
            "expiration_type": "standard",
            "strike": 5000,
            "bid": 10.0,
            "ask": 12.0,
            "last": 11.0,
            "change": -1.0,
            "change_percentage": -8.33,
            "volume": 100,
            "open_interest": 200,
            "bidsize": 5,
            "asksize": 5,
            "high": 13.0,
            "low": 9.5,
            "open": 12.5,
            "close": 12.0,
            "greeks": {
                "delta": -0.5,
                "gamma": 0.01,
                "theta": -0.03,
                "vega": 0.12,
                "rho": -0.1,
                "bid_iv": 0.2,
                "ask_iv": 0.22,
                "mid_iv": 0.21,
                "smv_vol": 0.23,
            },
        }
    ]
    timestamp = datetime.utcnow()
    expiration = "2024-05-03"
    underlying_price = {"last": 5050.0}

    uploader.upload_to_bigquery(options, timestamp, expiration, underlying_price)

    mock_to_gbq.assert_called_once()
    df_arg = mock_to_gbq.call_args[0][0]
    assert isinstance(df_arg, pd.DataFrame)
    assert df_arg.iloc[0]["symbol"] == "SPX240503P05000000"
    assert df_arg.iloc[0]["underlying_price"] == 5050.0


@patch("app.uploader.to_gbq")
def test_upload_index_price(mock_to_gbq):
    quote = {
        "last": 5050.0,
        "high": 5060.0,
        "low": 5040.0,
        "open": 5045.0,
        "close": 5055.0,
        "volume": 123456,
    }

    uploader.upload_index_price("SPX", quote)

    mock_to_gbq.assert_called_once()
    df_arg = mock_to_gbq.call_args[0][0]
    assert isinstance(df_arg, pd.DataFrame)
    assert df_arg.iloc[0]["symbol"] == "SPX"
    assert df_arg.iloc[0]["last"] == 5050.0
