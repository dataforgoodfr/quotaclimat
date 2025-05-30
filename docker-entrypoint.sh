#!/bin/bash

# Run migrations before starting the application
echo "Running migrations with alembic if exists"
poetry run alembic upgrade head


echo "update program metadata file"
poetry run python3 transform_program.py
if [[ $? -eq 0 ]]; then
    echo "Command succeeded"
else
    echo "Command failed"
fi

echo "starting mediatree import app"
python quotaclimat/data_processing/mediatree/api_import.py

echo "apply dbt models full-refresh"
poetry run dbt run --full-refresh # drop and recreate materialized views with the most up-to-date data.