# =====================
# app/utils.py
# Common utility functions like logging setup and market hours check
# =====================
import logging
import os
from datetime import datetime


def setup_logging():
    os.makedirs("logs", exist_ok=True)

    # File handler
    file_handler = logging.FileHandler("logs/fetcher.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    # Root logger
    logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])


def is_market_open(now=None):
    if now is None:
        now = datetime.utcnow()
    is_weekday = now.weekday() < 5
    is_open_time = (now.hour > 13 or (now.hour == 13 and now.minute >= 30)) and now.hour < 20
    return is_weekday and is_open_time
