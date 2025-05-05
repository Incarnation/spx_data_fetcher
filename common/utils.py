# =====================
# common/utils.py
# Common utility functions like logging setup and market hours check
# =====================
import logging
import os
from datetime import datetime

import pytz


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


def is_trading_hours():
    now = datetime.now(pytz.timezone("US/Eastern"))
    return (
        now.weekday() < 5
        and (now.hour > 9 or (now.hour == 9 and now.minute >= 30))
        and now.hour < 16
    )
