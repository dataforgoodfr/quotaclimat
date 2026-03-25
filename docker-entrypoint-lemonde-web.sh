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

# Check Python and alembic are available
python --version
alembic --version || true

# Run Alembic migrations (including lemonde_ftp_articles table)
echo "Run migrations to ensure database is up to date"
alembic -c alembic_factiva.ini upgrade head

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