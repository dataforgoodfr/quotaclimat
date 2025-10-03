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

echo "apply dbt models - except causal links"
poetry run dbt run --full-refresh --exclude core_query_causal_links

echo "Causal query case: Checking if today is the first of the month..."
day=$(date +%d)

if [ "$day" -eq 01 ]; then
  echo "✅ It's the 1st — running DBT for the previous month"

  # previous month (first day)
  prev_month=$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m-01)

  echo "Processing month: $prev_month"
  poetry run dbt run --select core_query_causal_links --vars "{\"process_month\": \"$prev_month\"}"
else
  echo "⏭️ Not the 1st — skipping DBT run"
fi
