#!/bin/bash
set -euo pipefail

PIPELINE="${1:-${DEFAULT_PIPELINE:-}}"

if [ -z "$PIPELINE" ]; then
  echo "Usage: docker-entrypoint-press-ingestion.sh <pipeline>"
  echo "Available pipelines: lemonde, ouest_france"
  exit 1
fi

cd /app
echo "============================================"
echo "Running Factiva migrations with alembic"
echo "============================================"
alembic -c alembic_factiva.ini upgrade head

case "$PIPELINE" in
  lemonde)
    echo "============================================"
    echo "STEP 1: Downloading Le Monde articles from FTP to S3"
    echo "============================================"
    python quotaclimat/data_ingestion/lemonde_ftp/ftp_to_s3.py

    echo "============================================"
    echo "STEP 2: Processing Le Monde articles from S3 to PostgreSQL"
    echo "============================================"
    python quotaclimat/data_ingestion/lemonde_ftp/s3_to_postgre/s3_lemonde_to_postgre.py

    echo "============================================"
    echo "Le Monde FTP pipeline complete"
    echo "============================================"
    ;;
  ouest_france)
    echo "============================================"
    echo "Fetching OuestFrance articles from API and uploading to S3"
    echo "============================================"
    python quotaclimat/data_ingestion/ouest_france/api_to_s3.py

    echo "============================================"
    echo "OuestFrance API to S3 pipeline complete"
    echo "============================================"
    ;;
  *)
    echo "Unknown pipeline: $PIPELINE"
    echo "Available pipelines: lemonde, ouest_france"
    exit 1
    ;;
esac
