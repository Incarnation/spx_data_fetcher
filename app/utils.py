# =====================
# app/utils.py
# Common utility functions like logging setup and market hours check
# =====================
import os
import logging
from datetime import datetime


def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        filename="logs/fetcher.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def is_market_open(now=None):
    if now is None:
        now = datetime.utcnow()
    is_weekday = now.weekday() < 5
    is_open_time = (
        now.hour > 13 or (now.hour == 13 and now.minute >= 30)
    ) and now.hour < 20
    return is_weekday and is_open_time
