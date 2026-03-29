#!/bin/bash
set -euo pipefail

cd /app
echo "============================================"
echo "Running Factiva migrations with alembic"
echo "============================================"

# Run Alembic migrations (includes OUESTFR source_classification insert)
alembic -c alembic_factiva.ini upgrade head

echo "============================================"
echo "Fetching OuestFrance articles from API and uploading to S3"
echo "============================================"
python quotaclimat/data_ingestion/ouest_france/api_to_s3.py

echo "============================================"
echo "OuestFrance API to S3 pipeline complete"
echo "============================================"
