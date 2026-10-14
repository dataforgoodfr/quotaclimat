#!/bin/bash

set -e

# Run migrations before starting the application (tables advertising.ad_merge*)
echo "Running migrations with alembic if exists"
poetry run alembic upgrade head

echo "============================================"
echo "Starting advertising split ads merge"
echo "============================================"

python quotaclimat/data_ingestion/advertising/s04_consolidation/e01_merge_split_ads/run.py

echo "============================================"
echo "Advertising split ads merge complete"
echo "============================================"
