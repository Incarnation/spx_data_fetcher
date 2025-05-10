# =====================
# common/utils.py
# Common utility functions like logging setup and market hours check
# =====================
import logging
import os
from datetime import datetime, time

import pytz


def setup_logging():
    os.makedirs("logs", exist_ok=True)

    # File handler
    # file_handler = logging.FileHandler("logs/fetcher.log")
    # file_handler.setLevel(logging.INFO)
    # file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    # Root logger
    logging.basicConfig(level=logging.INFO, handlers=[console_handler])


def is_trading_hours() -> bool:
    """
    True from 9:30 AM through 4:00 PM Eastern, Mon–Fri,
    excluding U.S. federal holidays.
    """
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    # 1) Mon–Fri only
    if now.weekday() >= 5:
        return False

    # 3) Market open/close inclusive
    return time(9, 30) <= now.time() <= time(16, 1)  # allow up to 16:00:59
