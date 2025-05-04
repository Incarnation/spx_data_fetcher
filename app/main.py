# =====================
# app/main.py
# Entry point for FastAPI app and scheduler
# =====================
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI

from app.fetcher import (fetch_option_chain, fetch_underlying_price,
                         get_next_expirations)
from app.scheduler import start_scheduler
from app.uploader import upload_to_bigquery
from app.utils import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    start_scheduler()
    yield  # Wait for app shutdown


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"status": "running", "refresh": "every 15 minutes during trading hours"}


@app.get("/manual-fetch")
def manual_fetch():
    now = datetime.utcnow()
    expirations = get_next_expirations()
    if not expirations:
        return {"status": "no expirations found"}

    underlying_price = fetch_underlying_price()
    logs = []

    for expiry in expirations:
        data = fetch_option_chain(expiration=expiry)
        if data:
            upload_to_bigquery(data, now, expiry, underlying_price)
            logs.append(f"Uploaded {len(data)} rows for {expiry}")
        else:
            logs.append(f"No data for {expiry}")
    return {"status": "manual fetch complete", "details": logs}
