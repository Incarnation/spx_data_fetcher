# =============================
# Stage 1: Builder (optional if you want minimal final image)
# =============================
FROM python:3.11-slim AS base

# Set working directory
WORKDIR /app

# Install system dependencies for BigQuery, SSL, etc.
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# =============================
# Stage 2: Final Image
# =============================
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=base /usr/local/lib/python3.11 /usr/local/lib/python3.11
COPY --from=base /usr/local/bin /usr/local/bin

# Copy your application code
COPY . .

# Set environment to production
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Default command: run background scheduler
CMD ["python", "workers/main.py"]
