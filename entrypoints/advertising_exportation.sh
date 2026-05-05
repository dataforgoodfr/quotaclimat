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
echo "Starting advertising exportation pipeline"
echo "============================================"

python quotaclimat/data_ingestion/advertising/s02_exportation/run.py

echo "============================================"
echo "Advertising exportation pipeline complete"
echo "============================================"
