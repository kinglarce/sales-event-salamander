#!/usr/bin/env bash
set -euo pipefail
cd /app

if [[ -f /app/.env ]]; then
  set -a
  # shellcheck source=/dev/null
  source /app/.env
  set +a
fi

echo "=== Starting scheduled run $(date -u +"%Y-%m-%dT%H:%M:%SZ") ===" >> /app/logs/cron.log
python v1/run_ingest.py >> /app/logs/cron.log 2>&1
