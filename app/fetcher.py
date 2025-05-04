# =====================
# app/fetcher.py
# Fetches SPX option expiration dates and chains from Tradier
# =====================
import httpx
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("TRADIER_API_KEY")
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}

def get_next_expirations(symbol="SPX", count=30):
    url = "https://api.tradier.com/v1/markets/options/expirations"
    params = {"symbol": symbol, "includeAllRoots": "true"}
    try:
        with httpx.Client() as client:
            resp = client.get(url, headers=HEADERS, params=params)
            data = resp.json()
            return data.get("expirations", {}).get("date", [])[:count]
    except Exception as e:
        import logging
        logging.error(f"Error fetching expirations: {e}")
        return []

def fetch_option_chain(symbol="SPX", expiration="2025-05-05"):
    url = "https://api.tradier.com/v1/markets/options/chains"
    params = {"symbol": symbol, "expiration": expiration, "greeks": "true"}
    try:
        with httpx.Client() as client:
            resp = client.get(url, headers=HEADERS, params=params)
            data = resp.json()
            return data.get("options", {}).get("option", [])
    except Exception as e:
        import logging
        logging.error(f"Error fetching option chain for {expiration}: {e}")
        return []