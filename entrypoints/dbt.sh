#!/bin/bash

echo "ingest labelstudio data into barometre database"
poetry run python -m quotaclimat.data_ingestion.labelstudio.ingest_labelstudio

echo "apply dbt models - except causal links and analytics tables"
poetry run dbt run --full-refresh \
--exclude core_query_causal_links \
--exclude task_global_completion \
--exclude environmental_shares_with_desinfo_counts

echo "apply dbt models to build analytics tables in 'analytics' schema."
poetry run dbt run --full-refresh --target analytics \
--select task_global_completion \
--select environmental_shares_with_desinfo_counts