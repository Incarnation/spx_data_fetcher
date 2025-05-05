# =====================
# Dockerfile
# =====================
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Allow absolute imports from root (e.g., `from app.fetcher import ...`)
ENV PYTHONPATH=/app

# Default entrypoint for Dash (can be overridden in Railway service settings)
CMD ["python", "app.py"]
