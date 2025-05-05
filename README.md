# 🧠 SPX + ETF Option Chain Data Fetcher & Analytics Pipeline

A production-grade Python service to fetch **SPX, SPY, QQQ, and NDX** option chain data from **Tradier** every 10 minutes **during U.S. trading hours**, upload to **BigQuery**, and compute:

- 🧮 **Gamma Exposure (GEX)** by strike and expiration  
- 📊 **Realized Volatility** (1H and 1D) using 5-min index price snapshots

---

## 🏗 Project Structure

```
spx_data_fetcher/
├── app/
│   ├── main.py
│   ├── scheduler.py
│   ├── fetcher.py
│   ├── uploader.py
│   └── utils.py
├── analytics/
│   ├── gex_calculator.py
│   └── realized_vol.py
├── common/
│   └── is_trading_hours.py
├── workers/
│   └── main.py
├── requirements.txt
├── Dockerfile
└── .env
```

---

## ⚙️ Local Setup

1. Create a `.env` file:

```env
TRADIER_API_KEY=your_tradier_api_key
GOOGLE_CLOUD_PROJECT=your_project_id
GOOGLE_APPLICATION_CREDENTIALS=etc/secrets/gcp-service-account.json
OPTION_CHAINS_TABLE_ID=your_project.options.option_chain_snapshot
INDEX_PRICE_TABLE_ID=your_project.market_data.index_price_snapshot
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run locally:

```bash
make run
# or manually:
PYTHONPATH=. python3 workers/main.py
```

---

## 🐳 Docker (Optional)

```bash
docker build -t spx-fetcher .
docker run --env-file .env spx-fetcher
```

---

## 🚀 Deploy on Render (Background Worker)

- **Service Type**: Background Worker
- **Build Command**: `pip install -r requirements.txt`
- **Start Command**: `PYTHONPATH=. python3 workers/main.py`
- **Environment Variables**:
  - `TRADIER_API_KEY`
  - `GOOGLE_CLOUD_PROJECT`
  - `GOOGLE_APPLICATION_CREDENTIALS`
  - `OPTION_CHAINS_TABLE_ID`
  - `INDEX_PRICE_TABLE_ID`

> Upload `gcp-service-account.json` to `/etc/secrets/` and set:
> `GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/gcp-service-account.json`

---

## ⏱ Scheduled Jobs

| Job                            | Schedule       | Description                            |
|-------------------------------|----------------|----------------------------------------|
| `scheduled_fetch`             | every 10 mins  | Fetch option chains and quotes         |
| `calculate_and_store_gex`     | every 10 mins  | Compute and store gamma exposure       |
| `calculate_and_store_realized_vol` | every 10 mins  | Compute 1H and 1D realized vol         |
| `debug_heartbeat`             | every 1 min    | Scheduler liveness check               |

---

## 📊 Output Tables

- `analytics.gamma_exposure`
- `analytics.realized_volatility`
- `options.option_chain_snapshot`
- `market_data.index_price_snapshot`

---

## 🛠 API (Optional)

- `GET /` → Health check
- `GET /manual-fetch` → Manual on-demand fetch

---

## 🪵 Logging

- Logs saved to `logs/fetcher.log` and stdout
- Includes fetches, uploads, scheduler events, and analytics

---

## ✅ Supported Symbols

- `SPX`, `SPY`, `QQQ`, `NDX` (easy to extend)

---

## 🧪 Testability

- Scheduler can be run standalone as `PYTHONPATH=. python3 workers/main.py`
- All analytics can be invoked independently
