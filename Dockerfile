# FiniexDataCollector Dockerfile
# ==============================

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (cache layer)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set Python path
ENV PYTHONPATH=/app

# Create data directories
RUN mkdir -p /app/data/raw/kraken \
    && mkdir -p /app/data/processed/kraken \
    && mkdir -p /app/configs/brokers/kraken \
    && mkdir -p /app/logs \
    && mkdir -p /app/output

# Default command: start collector
CMD ["python", "python/main.py", "collect"]
