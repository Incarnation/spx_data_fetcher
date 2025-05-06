# =====================
# fetcher/main.py
# Entry point for FastAPI app and scheduler with lifespan
# =====================

"""
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI

from app.fetcher import (
    SUPPORTED_SYMBOLS,
    fetch_option_chain,
    fetch_underlying_quote,
    get_next_expirations,
)
from app.uploader import upload_index_price, upload_to_bigquery

from .scheduler import shutdown_scheduler, start_scheduler
from .utils import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    start_scheduler()
    yield  # startup complete
    shutdown_scheduler()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"status": "running", "refresh": "every 10 minutes during trading hours"}


@app.get("/manual-fetch")
def manual_fetch():
    logs = []
    now = datetime.utcnow()
    for symbol in SUPPORTED_SYMBOLS:
        expirations = get_next_expirations(symbol)
        underlying_price = fetch_underlying_quote(symbol)
        upload_index_price(symbol, underlying_price)

        for expiry in expirations:
            data = fetch_option_chain(symbol, expiry)
            if data:
                upload_to_bigquery(data)
                logs.append(f"Uploaded {len(data)} rows for {symbol} {expiry}")
            else:
                logs.append(f"No data for {symbol} {expiry}")

    return {"status": "manual fetch complete", "details": logs}
"""
