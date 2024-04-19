#!/bin/bash

# Run migrations before starting the application
echo "Running migrations with alembic if exists"
poetry run alembic upgrade head

echo "starting mediatree import app"
python quotaclimat/data_processing/mediatree/api_import.py