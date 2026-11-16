#!/bin/bash

set -e
# Run migrations before starting the application
echo "Running migrations with alembic if exists"
poetry run alembic upgrade head

if [[ $? -eq 0 ]]; then
    echo "Command succeeded"
else
    echo "Command failed"
fi

echo "============================================"
echo "Starting advertising brand groups proposals"
echo "============================================"

python quotaclimat/data_ingestion/advertising/s05_enrichment/e01_brand_groups/run.py

echo "============================================"
echo "Advertising brand groups proposals complete"
echo "============================================"
