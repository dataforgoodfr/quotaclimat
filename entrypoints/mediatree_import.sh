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

echo "ingest labelstudio data into barometre database"
poetry run python -m quotaclimat.data_ingestion.labelstudio.ingest_labelstudio

echo "download reference Google Sheets as dbt seeds, then load and test them"
poetry run python -m quotaclimat.data_ingestion.external_sources.download_external_sources
poetry run dbt seed --full-refresh --select path:seeds/ref
poetry run dbt test --select path:seeds/ref

echo "apply dbt models - except analytics tables"
poetry run dbt run --full-refresh \
  --exclude task_global_completion \
  --exclude environmental_shares_with_desinfo_counts \
  --exclude path:models/advertising \
  --exclude cas_de_mesinformation \
  --exclude publicites

echo "apply dbt models to build analytics tables in 'analytics' schema."
poetry run dbt run --full-refresh --target analytics \
  --select task_global_completion \
  --select environmental_shares_with_desinfo_counts

echo "apply advertising dbt models and the analytics tables built on them (publicites, cas_de_mesinformation), after the analytics tables they read (misinformation distance)"
poetry run dbt run --full-refresh \
  --select path:models/advertising \
  --select cas_de_mesinformation \
  --select publicites
