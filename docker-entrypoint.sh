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

echo "apply dbt models"

     # drop and recreate materialized views with the most up-to-date data.

echo "apply dbt incremental model core_query_causal_links for each month"
for m in {2022-01..2025-09}; do
  echo "Processing month: $m"
  poetry run dbt run -m core_query_causal_links --vars "{\"process_month\": \"$m-01\"}"
done

