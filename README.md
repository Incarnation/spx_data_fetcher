# 🧠 SPX + ETF Option Chain Data Fetcher & Gamma Dashboard

A production-grade Python service that fetches **SPX, SPY, QQQ, and NDX** option chain data from **Tradier** every 10 minutes **during U.S. trading hours**, uploads it to **BigQuery**, and computes:

- 🧮 **Gamma Exposure (GEX)** by strike and expiration
- 📊 **Realized Volatility** (1H and 1D) using 5-min index price snapshots
- 📈 **Interactive Gamma Exposure Dashboard** built with Plotly Dash

---

## 🏗 Project Structure

```
spx_data_fetcher/
├── app/                 # Core logic for fetching & uploading
│   ├── fetcher.py
│   ├── uploader.py
│   ├── scheduler.py
│   ├── utils.py
├── analytics/           # Analytics logic for GEX and RVOL
│   ├── gex_calculator.py
│   └── realized_vol.py
├── common/              # Shared utilities and auth
│   ├── auth.py
│   └── is_trading_hours.py
├── dashboard/           # Gamma exposure dashboard (Dash)
│   ├── app.py
│   └── utils/bq_queries.py
├── workers/             # Background scheduler runner
│   └── main.py
├── etc/secrets/         # GCP JSON service account file
├── requirements.txt
├── Dockerfile
├── .env
└── railway.json
```

---

## ⚙️ Local Setup

1. Create a `.env` file:

```env
TRADIER_API_KEY=your_tradier_api_key
GOOGLE_CLOUD_PROJECT=your_project_id
GOOGLE_APPLICATION_CREDENTIALS=etc/secrets/gcp-service-account.json
GOOGLE_SERVICE_ACCOUNT_JSON={"type": "service_account", ...}
OPTION_CHAINS_TABLE_ID=your_project.options.option_chain_snapshot
INDEX_PRICE_TABLE_ID=your_project.market_data.index_price_snapshot
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the background worker:

```bash
make run
# or manually:
PYTHONPATH=. python3 workers/main.py
```

4. Run the dashboard locally:

```bash
PYTHONPATH=. python3 dashboard/app.py
```

---

## 🐳 Docker (Optional)

```bash
docker build -t spx-dashboard .
docker run --env-file .env -p 8050:8050 spx-dashboard
```

---

## 🚀 Deploy on Railway

This project deploys **two services**:

### 1. Background Worker
- **Start Command**: `python workers/main.py`
- **Type**: Background Worker

### 2. Dashboard (Web App)
- **Start Command**: `python dashboard/app.py`
- **Type**: Web Service (port `8050`)

**railway.json** is already configured with both services:
```json
{
  "services": [
    {
      "name": "worker",
      "startCommand": "python workers/main.py"
    },
    {
      "name": "dashboard",
      "startCommand": "python dashboard/app.py",
      "ports": [8050]
    }
  ]
}
```

> After deployment, click **"Generate Domain"** under the dashboard service to make it publicly accessible.

---

## ⏱ Scheduled Jobs

| Job                            | Schedule       | Description                            |
|-------------------------------|----------------|----------------------------------------|
| `scheduled_fetch`             | every 10 mins  | Fetch option chains and quotes         |
| `calculate_and_store_gex`     | every 7 mins   | Compute and store gamma exposure       |
| `calculate_and_store_realized_vol` | every 7 mins   | Compute 1H and 1D realized vol         |
| `debug_heartbeat`             | every 2 mins   | Scheduler liveness check               |

---

## 📊 BigQuery Tables

- `analytics.gamma_exposure`
- `analytics.realized_volatility`
- `options.option_chain_snapshot`
- `market_data.index_price_snapshot`

---

## 📈 Dashboard Features

- Interactive gamma exposure chart by strike
- Select expiration date from dropdown
- Red bars for negative GEX, blue for positive
- Spot price line overlayed

---

## 🛠 API (Optional)

- `GET /` → Health check
- `GET /manual-fetch` → Manual on-demand fetch

---

## 🧪 Testability

- Run scheduler and analytics independently
- Run dashboard locally with live data

---

## ✅ Supported Symbols

- `SPX`, `SPY`, `QQQ`, `NDX` (easy to extend)
