# =====================
# fetcher/fetcher.py
# Refactored to support multiple symbols: SPX
# =====================
import logging

import requests

from common.config import BASE_URL, TRADIER_API_KEY


def get_auth_headers():
    if not TRADIER_API_KEY:
        logging.error("🚫 TRADIER_API_KEY is missing! Make sure it's set in your environment.")
        return {}
    return {"Authorization": f"Bearer {TRADIER_API_KEY}", "Accept": "application/json"}


def fetch_underlying_quote(symbol: str) -> dict:
    try:
        resp = requests.get(
            f"{BASE_URL}/quotes", headers=get_auth_headers(), params={"symbols": symbol}
        )
        resp.raise_for_status()
        return resp.json().get("quotes", {}).get("quote", {})
    except Exception as e:
        logging.error(f"[FETCH ERROR] Unable to fetch quote for {symbol}: {e}")
        return {}


def get_next_expirations(symbol: str, limit: int = 20):
    try:
        resp = requests.get(
            f"{BASE_URL}/options/expirations",
            headers=get_auth_headers(),
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
            headers=get_auth_headers(),
            params={"symbol": symbol, "expiration": expiration, "greeks": "true"},
        )
        resp.raise_for_status()
        options = resp.json().get("options", {}).get("option", [])

        # Compute mid_price for each option leg
        for opt in options:
            bid = opt.get("bid") or 0.0
            ask = opt.get("ask") or 0.0
            opt["mid_price"] = (bid + ask) / 2

        # Filter 200 strikes closest to current price
        options = sorted(options, key=lambda x: abs(x.get("strike", 0.0) - current_price))
        return options[:200]

    except Exception as e:
        logging.error(f"[FETCH ERROR] Unable to fetch option chain for {symbol} {expiration}: {e}")
        return []
