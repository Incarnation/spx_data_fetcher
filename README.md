# 🧠 SPX + ETF Option Chain Data Fetcher & Multi‑Strategy Trading Dashboard

A robust Python service that:

1. Fetches **SPX, SPY, QQQ, NDX** option chain data from **Tradier** every 10 min during U.S. trading hours.
2. Uploads raw snapshots to **BigQuery**.
3. Computes:
   - 🧮 **Gamma Exposure (GEX)** by strike & expiry.
   - 📊 **Realized Volatility** (1 H & 1 D) using 5 min index snapshots.
   - 🔄 **Trade Recommendations** (0DTE Iron Condors & Spreads).
   - 💰 **Live PnL Monitoring** (per‑leg snapshots + EOD closure).
   - 📈 **P/L Analysis** (max P, max L, breakevens, PoP, Δ, Θ).
   - 🎯 **P/L Projections & Payoff Grid** (interactive slider in dashboard).

---

## 🏗 Project Structure

```
spx_data_fetcher/
├── common/              
│   ├── auth.py
│   ├── config.py
│   └── utils.py         
│
├── app/                 
│   ├── fetcher.py       
│   ├── uploader.py      
│   └── scheduler.py     
│
├── analytics/           
│   ├── gex_calculator.py
│   └── realized_vol.py
│
├── trade/               
│   ├── trade_generator.py  
│   ├── pl_analysis.py      
│   └── pnl_monitor.py      
│
├── dashboard/           
│   └── main.py          
│
├── workers/             
│   └── main.py
├── requirements.txt
├── Dockerfile
├── .env
└── railway.json         
```

---

## ⚙️ Environment Variables

```
# Tradier API
TRADIER_API_KEY=your_tradier_api_key

# Google / BigQuery
GOOGLE_CLOUD_PROJECT=your-gcp-project
GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json
GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account",…}'
OPTION_CHAINS_TABLE_ID=your-project.options.option_chain_snapshot
INDEX_PRICE_TABLE_ID=your-project.market_data.index_price_snapshot

# Trade generator defaults (optional overrides)
CONDOR_TARGET_DELTA=0.10
CONDOR_WING_WIDTH=10

# Model / code version (for audit)
MODEL_VERSION=v1.0-ironcondor
```

---

## 🐳 Docker

```
docker build -t spx-dashboard .
docker run --env-file .env -p 8050:8050 spx-dashboard
```

---

## 🧪 Local Setup

1. **Install dependencies**  
```
pip install -r requirements.txt
```

2. **Run background worker**  
```
PYTHONPATH=. python3 workers/main.py
```

3. **Run dashboard**  
```
PYTHONPATH=. python3 dashboard/main.py
```

---

## 🚀 Railway Deployment

`railway.json` defines two services:

1. **worker**  
   - Start: `python workers/main.py`  
   - Type: Background Worker  
2. **dashboard**  
   - Start: `python dashboard/main.py`  
   - Ports: `8050`  

After deploy, “Generate Domain” on dashboard service to expose publicly.

---

## ⏱ Scheduled Jobs

```
| Job                          | Schedule                | Description                              |
|------------------------------|-------------------------|----------------------------------------|
| scheduled_upload_index_price | 5 min, 9:30–16:00 ET    | Fetch & upload SPX index quotes         |
| scheduled_fetch_and_upload_options_data | 10 min, 9:30–16:00 ET | Fetch & upload option chains + Greeks |
| calculate_and_store_gex      | every 15 min, 9:30–16:00 ET | Compute & store gamma exposure         |
| calculate_and_store_realized_vol | every 5 min, 9:30–16:00 ET | Compute 1H & 1D realized volatility  |
| generate_0dte_trade          | 10:00, 11:00, 12:00, 13:00 ET | Auto-generate 0DTE Iron Condor/Spread |
| update_trade_pnl             | every 5 min 9:00–15:55 ET + 16:00 ET | Live PnL snapshots + final EOD closure |
| debug_heartbeat              | every 10 min (24/7)     | Scheduler liveness check                |
```

---

## 📊 BigQuery Tables

### Raw Snapshots
```
- options.option_chain_snapshot  
- market_data.index_price_snapshot
```

### Analytics
```
- analytics.gamma_exposure  
- analytics.realized_volatility
```

### Trading
```
- analytics.trade_recommendations  
- analytics.trade_legs  
- analytics.live_trade_pnl  
- analytics.trade_pl_analysis  
- analytics.trade_pl_projections
```

---

## 📈 Dashboard Features

1. **Gamma Exposure Surface:**  
   - 3D view of strike × expiry × net GEX.

2. **Gamma Exposure Analysis:**  
   - Bar chart by strike for chosen expiry.

3. **Trade Recommendations:**  
   - List of pending / active / closed auto‑generated trades.

4. **Live PnL Monitoring:**  
   - Leg‑level PnL snapshots every 5 min + EOD close.

5. **P/L Analysis & Projections:**  
   - **Static Analysis:** max P, max L, breakeven(s), PoP, Δ, Θ.  
   - **Interactive Payoff Grid:** drag slider to see how P/L changes at different underlying prices.  
   - **Historical P/L Projections:** line chart of P/L over time.

---

## ✅ Supported Symbols

```
- SPX, SPY, QQQ, NDX (easily extensible to other symbols).
```

---

Enjoy! 🎉
