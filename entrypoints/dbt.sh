#!/bin/bash

echo "ingest labelstudio data into barometre database"
poetry run python -m quotaclimat.data_ingestion.labelstudio.ingest_labelstudio

echo "download reference Google Sheets as dbt seeds, then load and test them"
poetry run python -m quotaclimat.data_ingestion.external_sources.download_external_sources
poetry run dbt seed --full-refresh --select path:seeds/ref
poetry run dbt test --select path:seeds/ref

echo "load the methodology seeds versioned in the repository (keywords of the misinformation themes), then test them"
poetry run dbt seed --full-refresh --select path:seeds/themes
poetry run dbt test --select path:seeds/themes

echo "apply dbt models - except analytics tables"
poetry run dbt run --full-refresh \
--exclude task_global_completion \
--exclude environmental_shares_with_desinfo_counts \
--exclude path:models/advertising \
--exclude cas_de_mesinformation \
--exclude publicites \
--exclude emission_jour \
--exclude pub_annonceur_emission_jour \
--exclude cas_theme \
--exclude path:models/curate

echo "apply dbt models to build the analytics tables (task_global_completion in 'analytics', environmental_shares_with_desinfo_counts in 'public')."
poetry run dbt run --full-refresh --target analytics \
--select task_global_completion \
--select environmental_shares_with_desinfo_counts

echo "apply advertising dbt models and the analytics tables built on them (publicites, cas_de_mesinformation), after the analytics tables they read (misinformation distance), then the daily aggregates built on them (emission_jour, pub_annonceur_emission_jour) and the themes of the cases (cas_theme)"
poetry run dbt run --full-refresh \
--select path:models/advertising \
--select cas_de_mesinformation \
--select publicites \
--select emission_jour \
--select pub_annonceur_emission_jour \
--select cas_theme

# the curate tables (models/curate) are built after this script by entrypoints/curate.sh, in its own Kestra task
