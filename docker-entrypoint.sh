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
if [ $REPARSE_CAUSAL_LINKS -eq 1 ]; then
  echo "Reparsing core_query_causal_links"
  year_end=$(date +%d)

  for m in $(seq 2025 2025); do
    start_reparse=0
    for mm in $(seq -w 1 12); do
      date="$m-$mm-01"
      echo "Processing month: $date"
      if [ $start_reparse -eq 0 ]; then
        poetry run dbt run --full-refresh --select core_query_causal_links --vars "{\"process_month\": \"$date\"}"
        start_reparse=1
      else
        poetry run dbt run --select core_query_causal_links --vars "{\"process_month\": \"$date\"}"
      fi
    done
  done
else
  echo "starting mediatree import app"
  python quotaclimat/data_processing/mediatree/api_import.py

  echo "ingest labelstudio data into barometre database"
  poetry run python -m quotaclimat.data_ingestion.labelstudio.ingest_labelstudio

  echo "apply dbt models - except causal links and analytics tables"
  poetry run dbt run --full-refresh --exclude core_query_causal_links --exclude task_global_completion

  echo "apply dbt models to build analytics tables in 'analytics' schema."
  poetry run dbt run --full-refresh --target analytics --select task_global_completion

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

fi
