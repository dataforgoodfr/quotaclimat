#!/bin/bash

set pipefail
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
echo "Starting advertising detection pipeline"
echo "============================================"

python quotaclimat/data_ingestion/advertising/s01_detection/run.py

echo "============================================"
echo "Advertising detection pipeline complete"
echo "============================================"