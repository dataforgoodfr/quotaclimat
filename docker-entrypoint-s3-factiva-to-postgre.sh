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

# Run Alembic migrations
echo "Run migrations to ensure database is up to date"
alembic -c alembic_factiva.ini upgrade head

echo "============================================"
echo "Starting S3 Factiva to PostgreSQL processor"
python quotaclimat/data_processing/factiva/s3_to_postgre/s3_factiva_to_postgre.py