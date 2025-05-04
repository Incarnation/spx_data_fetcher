# =====================
# app/main.py
# Entry point for FastAPI app and scheduler
# =====================
from fastapi import FastAPI
from .scheduler import start_scheduler
from .utils import setup_logging
from app.fetcher import fetch_option_chain, get_next_expirations
from app.uploader import upload_to_bigquery
from datetime import datetime

app = FastAPI()

@app.on_event("startup")
def startup():
    setup_logging()
    start_scheduler()

@app.get("/")
def read_root():
    return {"status": "running", "refresh": "every 10 minutes during trading hours"}

@app.get("/manual-fetch")
def manual_fetch():
    now = datetime.utcnow()
    expirations = get_next_expirations()
    if not expirations:
        return {"status": "no expirations found"}
    
    logs = []
    for expiry in expirations:
        data = fetch_option_chain(expiration=expiry)
        if data:
            upload_to_bigquery(data, now, expiry)
            logs.append(f"Uploaded {len(data)} rows for {expiry}")
        else:
            logs.append(f"No data for {expiry}")
    return {"status": "manual fetch complete", "details": logs}