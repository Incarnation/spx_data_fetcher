# =====================
# app/fetcher.py
# Refactored to support multiple symbols: SPX, SPY, QQQ, NDX
# =====================
import logging
import os
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

TRADIER_API_KEY = os.getenv("TRADIER_API_KEY")
HEADERS = {"Authorization": f"Bearer {TRADIER_API_KEY}", "Accept": "application/json"}
BASE_URL = "https://api.tradier.com/v1/markets"
CONTRACT_MULTIPLIER = 100
SUPPORTED_SYMBOLS = ["SPX", "SPY", "QQQ"]


def fetch_underlying_quote(symbol: str) -> dict:
    try:
        resp = requests.get(f"{BASE_URL}/quotes", headers=HEADERS, params={"symbols": symbol})
        resp.raise_for_status()
        return resp.json().get("quotes", {}).get("quote", {})
    except Exception as e:
        logging.error(f"[FETCH ERROR] Unable to fetch quote for {symbol}: {e}")
        return {}


def get_next_expirations(symbol: str, limit: int = 3):
    try:
        resp = requests.get(
            f"{BASE_URL}/options/expirations",
            headers=HEADERS,
            params={"symbol": symbol, "includeAllRoots": "true", "strikes": "false"},
        )
        resp.raise_for_status()
        return resp.json().get("expirations", {}).get("date", [])[:limit]
    except Exception as e:
        logging.warning(f"[FETCH WARNING] Failed to get expirations for {symbol}: {e}")
        return []


def fetch_option_chain(symbol: str, expiration: str):
    try:
        resp = requests.get(
            f"{BASE_URL}/options/chains",
            headers=HEADERS,
            params={"symbol": symbol, "expiration": expiration, "greeks": "true"},
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("options", {}).get("option", [])

    except Exception as e:
        logging.error(f"[FETCH ERROR] Unable to fetch option chain for {symbol} {expiration}: {e}")
        return []
