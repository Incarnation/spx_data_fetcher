# =====================
# Dockerfile
# =====================
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Allow absolute imports like `from common.auth import ...`
ENV PYTHONPATH=/app

# 🔧 Updated to run the dashboard correctly
CMD ["python", "dashboard/main.py"]
