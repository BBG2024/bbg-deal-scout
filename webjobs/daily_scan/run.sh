#!/bin/bash
# BBG Deal Scout — Azure WebJob: Daily Scan
# This runs automatically on schedule via Azure App Service WebJobs.
# Schedule is set in settings.job

cd /home/site/wwwroot
echo "=== BBG Deal Scout Daily Scan: $(date) ==="
python -m src.cli scan
python -m src.cli export
echo "=== Scan complete: $(date) ==="
