# tests/test_fetcher.py
from unittest.mock import patch

import pytest
import requests

from fetcher.fetcher import fetch_option_chain, fetch_underlying_quote, get_next_expirations

# =====================
# tests/test_fetcher.py
# Unit tests for fetcher functions
# =====================
# This file contains unit tests for the functions in fetcher.py.
# It uses pytest and unittest.mock to create mock responses for the API calls.
# The tests cover the following functions:
# - fetch_underlying_quote
# - get_next_expirations
# - fetch_option_chain

# The tests can be run using pytest tests/test_fetcher.py -v
# =====================


@pytest.fixture
def mock_quote():
    return {
        "quotes": {
            "quote": {
                "last": 420.0,
                "high": 425.0,
                "low": 415.0,
                "open": 418.0,
                "close": 419.0,
                "volume": 1200000,
            }
        }
    }


@pytest.fixture
def mock_expirations():
    return {"expirations": {"date": ["2025-05-06", "2025-05-07", "2025-05-08"]}}


@pytest.fixture
def mock_option_chain():
    return {
        "options": {
            "option": [
                {"strike": 410},
                {"strike": 415},
                {"strike": 420},
                {"strike": 425},
                {"strike": 430},
            ]
        }
    }


@patch("requests.get")
def test_fetch_underlying_quote(mock_get, mock_quote):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_quote

    result = fetch_underlying_quote("SPY")
    assert result["last"] == 420.0
    assert result["high"] == 425.0


@patch("requests.get")
def test_get_next_expirations(mock_get, mock_expirations):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_expirations

    dates = get_next_expirations("SPY", limit=2)
    assert dates == ["2025-05-06", "2025-05-07"]


@patch("requests.get")
def test_fetch_option_chain(mock_get, mock_option_chain, mock_quote):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_option_chain

    quote = mock_quote["quotes"]["quote"]
    chain = fetch_option_chain("SPY", "2025-05-06", quote)
    assert len(chain) == 5
    assert sorted(chain, key=lambda x: x["strike"])[0]["strike"] == 410


@patch("requests.get")
def test_fetch_option_chain_no_price(mock_get):
    quote = {}  # missing "last"
    chain = fetch_option_chain("SPY", "2025-05-06", quote)
    assert chain == []
