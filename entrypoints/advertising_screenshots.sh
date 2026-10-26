#!/bin/bash

set -e

# Run migrations before starting the application (table advertising.ad_screenshot)
echo "Running migrations with alembic if exists"
poetry run alembic upgrade head

echo "============================================"
echo "Starting advertising screenshots"
echo "============================================"

python quotaclimat/data_ingestion/advertising/s05_enrichment/e02_screenshots/run.py

echo "============================================"
echo "Advertising screenshots complete"
echo "============================================"
