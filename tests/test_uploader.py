# =====================
# tests/test_uploader.py
# =====================
from datetime import datetime

import pandas as pd
import pytest

from app import uploader


def test_upload_to_bigquery_mock(monkeypatch):
    mock_data = [
        {
            "symbol": "SPXW250505C05700000",
            "root_symbol": "SPXW",
            "option_type": "call",
            "expiration_date": "2025-05-05",
            "expiration_type": "weeklys",
            "strike": 5700.0,
            "bid": 18.9,
            "ask": 19.4,
            "last": 19.25,
            "change": 11.1,
            "change_percentage": 136.2,
            "volume": 7644,
            "open_interest": 2240,
            "bidsize": 11,
            "asksize": 23,
            "high": 29.0,
            "low": 12.0,
            "open": 18.06,
            "close": 19.25,
            "greeks": {
                "delta": 0.43,
                "gamma": 0.006,
                "theta": -3.7,
                "vega": 1.9,
                "rho": 0.16,
                "bid_iv": 0.11,
                "ask_iv": 0.12,
                "mid_iv": 0.115,
                "smv_vol": 0.119,
            },
        }
    ]

    # Mock the upload method to just return without error
    monkeypatch.setattr(uploader, "upload_to_bigquery", lambda options, timestamp, expiration: None)
    uploader.upload_to_bigquery(mock_data, datetime.utcnow(), "2025-05-05")
