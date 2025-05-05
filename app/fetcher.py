# =====================
# app/fetcher.py
# Refactored to support multiple symbols: SPX, SPY, QQQ, NDX
# =====================
import logging
import os

import requests
from dotenv import load_dotenv

# Load .env only if running locally (optional guard)
if not (os.getenv("RENDER") or os.getenv("RAILWAY_ENVIRONMENT")):
    from pathlib import Path

    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

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


def get_next_expirations(symbol: str, limit: int = 20):
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


def fetch_option_chain(symbol: str, expiration: str, quote: dict):
    try:
        current_price = quote.get("last")
        if current_price is None:
            logging.warning(f"⚠️ Missing current price for {symbol}, skipping strike filter.")
            return []

        resp = requests.get(
            f"{BASE_URL}/options/chains",
            headers=HEADERS,
            params={"symbol": symbol, "expiration": expiration, "greeks": "true"},
        )
        resp.raise_for_status()
        options = resp.json().get("options", {}).get("option", [])

        # Step 3: Filter 120 strikes closest to current price (±60)
        options = sorted(options, key=lambda x: abs(x.get("strike", 0) - current_price))
        return options[:120]

    except Exception as e:
        logging.error(f"[FETCH ERROR] Unable to fetch option chain for {symbol} {expiration}: {e}")
        return []
