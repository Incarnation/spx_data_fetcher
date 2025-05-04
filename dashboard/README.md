# SPX Gamma Exposure Dashboard (Dash)

This Dash app visualizes SPX gamma exposure by strike in real time using BigQuery option data.

## 📦 Setup

1. Create `.env` in project root:
   ```env
   GOOGLE_CLOUD_PROJECT=your-gcp-project-id
   GOOGLE_APPLICATION_CREDENTIALS=./gcp-service-account.json
   ```

2. Install dependencies:
   ```bash
   pip install -r dashboard/requirements.txt
   ```

3. Run locally:
   ```bash
   cd dashboard
   python app.py
   ```
   Visit http://localhost:8050

## 🚀 Deploy to Render

1. Push your repo to GitHub

2. Go to https://dashboard.render.com → New Web Service
   - Type: Web Service
   - Runtime: Python
   - Build Command:
     ```bash
     pip install -r dashboard/requirements.txt
     ```
   - Start Command:
     ```bash
     python dashboard/app.py
     ```

3. Add env vars:
   - `GOOGLE_CLOUD_PROJECT`
   - `GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/gcp.json`

4. Upload service account JSON as a **Secret File**:
   - Path: `/etc/secrets/gcp.json`

5. Done! App is live at your-subdomain.onrender.com