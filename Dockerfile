FROM python:3.11-slim

LABEL maintainer="info@bluebeargroup.ca"
LABEL description="BBG Deal Scout — Multifamily Deal Intelligence Platform"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create persistent data directories
RUN mkdir -p data/inbox data/inbox/processed logs

# Copy default config if no config.yaml is mounted
RUN cp config.yaml.example config.yaml

EXPOSE 8050

# Production: gunicorn with uvicorn workers
CMD ["gunicorn", "src.dashboard.app:app", \
     "-k", "uvicorn.workers.UvicornWorker", \
     "--bind", "0.0.0.0:8050", \
     "--workers", "2", \
     "--timeout", "120", \
     "--log-level", "info"]
