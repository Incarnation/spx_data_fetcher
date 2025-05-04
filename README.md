# =====================
# README.md
# =====================
# SPX Option Chain Data Fetcher

This service fetches SPX option chain data from Tradier every 10 minutes **during U.S. trading hours** and uploads it to BigQuery.

## Setup

1. Clone this repo
2. Create a `.env` file:
   ```
   TRADIER_API_KEY=your_tradier_api_key
   GOOGLE_CLOUD_PROJECT=your_gcp_project_id
   ```
3. Run locally:
   ```bash
   pip install -r requirements.txt
   uvicorn app.main:app --reload
   ```

## Deploy with Docker

```bash
docker build -t spx-fetcher .
docker run -p 8080:8080 --env-file .env spx-fetcher
```

## Deploy on Render.com

- Link GitHub repo
- Set build command: `pip install -r requirements.txt`
- Set start command: `uvicorn app.main:app --host 0.0.0.0 --port 8080`
- Add env vars: `TRADIER_API_KEY`, `GOOGLE_CLOUD_PROJECT`
- Select free instance

## API

- `GET /` → basic health check

## Logging

- Logs are stored in `logs/fetcher.log`
- Includes fetch timestamps, expiration targets, upload status