# FiniexDataCollector Dockerfile
# ==============================

FROM python:3.12-slim

# System-Pakete installieren (Git, Build-Tools und htop für Monitoring)
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    htop \
    && rm -rf /var/lib/apt/lists/*

# Git safe directory fix für VS Code
RUN git config --system --add safe.directory /app

# Set working directory
WORKDIR /app

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

# Bash als interaktive Login-Shell setzen
CMD ["/bin/bash", "-l"]