#!/bin/bash
# BBG Deal Scout — Azure App Service startup script
# Azure runs this file on every container start.

set -e

# Ensure persistent data directories exist on Azure's /home mount
mkdir -p /home/data/inbox/processed
mkdir -p /home/site/wwwroot/logs

# If no config.yaml exists, create from example
CONFIG="/home/site/wwwroot/config.yaml"
if [ ! -f "$CONFIG" ]; then
    cp /home/site/wwwroot/config.yaml.example "$CONFIG"
    echo "Created config.yaml from example"
fi

# Initialize the database (creates tables if they don't exist)
cd /home/site/wwwroot
python -c "from src.database import init_db; init_db('/home/data/deal_scout.db')" 2>/dev/null || true
python -c "from src.sources import init_source_tables; init_source_tables()" 2>/dev/null || true
python -c "from src.analyst.storage import init_analyst_tables; init_analyst_tables()" 2>/dev/null || true

echo "BBG Deal Scout startup complete."

# Start the web server on port 8000 (Azure default)
exec gunicorn src.dashboard.app:app \
    -k uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:8000 \
    --workers 2 \
    --timeout 120 \
    --log-level info \
    --access-logfile - \
    --error-logfile -
