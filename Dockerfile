# FiniexDataCollector Dockerfile
# ==============================

FROM python:3.12-slim

# System-Pakete installieren (Git, Build-Tools und htop für Monitoring)
# curl is required by the GitHub CLI install below, not optional tooling.
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    htop \
    && rm -rf /var/lib/apt/lists/*

# GitHub CLI — the issue snapshot in github_issues/ is fetched with it, so it belongs
# in the image rather than in whoever remembers to install it. Not in Debian stable,
# hence the vendor repository.
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
      -o /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
      > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update && apt-get install -y gh \
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