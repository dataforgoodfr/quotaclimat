#!/bin/bash
set -euo pipefail

cd /app
echo "============================================"
echo "Running Factiva migrations with alembic"
echo "PWD: $(pwd)"
echo "Listing alembic_factiva files:"
ls -la alembic_factiva || true
ls -la alembic_factiva/versions || true
echo "Using config file: alembic_factiva.ini"

poetry --version || true

poetry run alembic -c alembic_factiva.ini upgrade head

echo "============================================"
echo "Starting Factiva ingestion app"
python quotaclimat/data_ingestion/factiva/ingest_factiva_streaming.py
