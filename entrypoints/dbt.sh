#!/bin/bash

# Usage: dbt.sh [analytics]
# Without argument: all the models.
# With "analytics": leaves out the models of models/dashboards and models/homepage (computed from the whole
# public.keywords table, the long part of the run), which keep their last build. The other models (the tables read
# by the Metabase questions) are rebuilt as in a full run. Flow dbt-analytics.
#
# models/daily is the daily layer of the dashboards and homepage tables, incremental by day: a run only recomputes the
# recent days (my_dbt_project/macros/daily_window.sql). During the double run, it rebuilds next to them the same tables
# from its days, compared in double_run_comparison. Environment variables:
# - DBT_FULL_REFRESH=true: rebuilds the whole history of models/daily instead (after a change of its code, of the
#   program grid or of the dictionary, or of keywords older than the window). Kestra input full_refresh.
# - DBT_IMPORT_START_DATE (YYYY-MM-DD, default today in Paris) and DBT_IMPORT_DAYS_BACK: the period imported by
#   the flow that runs dbt; the window then starts at the latest on the day before that period.
if [[ "$1" == "analytics" ]]; then
    echo "analytics only: the models of models/dashboards and models/homepage are left out"
    skip_dashboards=(--exclude path:models/dashboards --exclude path:models/homepage)
else
    skip_dashboards=()
fi

if [[ "${DBT_FULL_REFRESH:-false}" == "true" ]]; then
    echo "full refresh: the models of models/daily are rebuilt over their whole history"
    full_refresh=(--full-refresh)
else
    full_refresh=()
fi

recompute_since=""
if [[ -n "${DBT_IMPORT_DAYS_BACK:-}" ]]; then
    import_start_date="${DBT_IMPORT_START_DATE:-$(TZ=Europe/Paris date +%F)}"
    recompute_since=$(date -d "${import_start_date} - $((DBT_IMPORT_DAYS_BACK + 1)) days" +%F) || exit 1
    echo "imported period: ${DBT_IMPORT_DAYS_BACK} days before ${import_start_date}, recomputed from ${recompute_since} at the latest"
fi

echo "ingest labelstudio data into barometre database"
poetry run python -m quotaclimat.data_ingestion.labelstudio.ingest_labelstudio

echo "download reference Google Sheets as dbt seeds, then load and test them"
poetry run python -m quotaclimat.data_ingestion.external_sources.download_external_sources
# --no-partial-parse: the CSV files just downloaded are new to a previous parse, which would leave out their
# column_types (my_dbt_project/seeds/ref/_ref_seeds.yml) and load a column of digits (SIREN) as an integer
poetry run dbt seed --full-refresh --no-partial-parse --select path:seeds/ref
poetry run dbt test --select path:seeds/ref

echo "load the methodology seeds versioned in the repository (keywords of the misinformation themes), then test them"
poetry run dbt seed --full-refresh --select path:seeds/themes
poetry run dbt test --select path:seeds/themes

echo "apply dbt models - except analytics tables and the daily layer"
poetry run dbt run --full-refresh "${skip_dashboards[@]}" \
--exclude path:models/daily \
--exclude task_global_completion \
--exclude environmental_shares_with_desinfo_counts \
--exclude path:models/advertising \
--exclude cas_de_mesinformation \
--exclude publicites \
--exclude emission_jour \
--exclude pub_annonceur_emission_jour \
--exclude cas_theme \
--exclude path:models/curated

if [[ "$1" != "analytics" ]]; then
    echo "apply the daily layer (models/daily, incremental by day), the tables computed from it and their comparison with models/dashboards and models/homepage (double run)"
    poetry run dbt run "${full_refresh[@]}" --vars "{recompute_since: '${recompute_since}'}" --select path:models/daily
    # warning only: a difference is reported in the logs and in double_run_comparison, it does not stop the pipeline
    poetry run dbt test --select double_run_identical
fi

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

# the curated tables (models/curated) are built after this script by entrypoints/curated.sh, in its own Kestra task
