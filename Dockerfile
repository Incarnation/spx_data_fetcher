# =====================
# Dockerfile
# =====================
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 👇 Add this so `import app.*` works
ENV PYTHONPATH=/app

# 👇 Set default entry point for worker
CMD ["python", "workers/main.py"]
