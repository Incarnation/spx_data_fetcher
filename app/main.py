# =====================
# app/main.py
# Entry point for FastAPI app and scheduler
# =====================
from fastapi import FastAPI
from .scheduler import start_scheduler
from .utils import setup_logging

app = FastAPI()

@app.on_event("startup")
def startup():
    setup_logging()
    start_scheduler()

@app.get("/")
def read_root():
    return {"status": "running", "refresh": "every 10 minutes during trading hours"}
