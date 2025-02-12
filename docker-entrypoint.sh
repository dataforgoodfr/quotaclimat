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

echo "apply dbt models"
poetry run dbt run

echo "starting mediatree import app"
python quotaclimat/data_processing/mediatree/api_import.py