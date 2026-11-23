#!/bin/bash

# Usage: dbt.sh [analytics] [full-refresh]
# Without argument: all the models, the incremental ones (models/dashboards, models/homepage) recompute only the last
# DBT_LOOKBACK_WEEKS weeks (1 by default: from the Monday of the previous week, see my_dbt_project/macros/incremental_window.sql).
# With "analytics": leaves out the models of models/dashboards and models/homepage (computed from the whole
# public.keywords table, the long part of the run), which keep their last build. The other models (the tables read
# by the Metabase questions) are rebuilt as in a full run. Flow dbt-analytics.
# With "full-refresh": the incremental models are rebuilt from the whole keywords table (about 35 minutes). Needed after
# keywords of past weeks were reprocessed beyond the window, a change of the program grid, or a change of a model.
# Done automatically on the first day of each month, as a safety net.
skip_dashboards=()
full_refresh=()
for arg in "$@"; do
    case "$arg" in
        analytics) skip_dashboards=(--exclude path:models/dashboards --exclude path:models/homepage) ;;
        full-refresh) full_refresh=(--full-refresh) ;;
    esac
done
if [[ "$(date -u +%d)" == "01" ]]; then
    full_refresh=(--full-refresh)
fi
if [[ ${#skip_dashboards[@]} -gt 0 ]]; then
    echo "analytics only: the models of models/dashboards and models/homepage are left out"
fi
if [[ ${#full_refresh[@]} -gt 0 ]]; then
    echo "full refresh of the incremental models"
fi
lookback_vars="{dashboards_lookback_weeks: ${DBT_LOOKBACK_WEEKS:-1}}"

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

echo "apply dbt models - except analytics tables"
poetry run dbt run "${full_refresh[@]}" --vars "$lookback_vars" "${skip_dashboards[@]}" \
--exclude task_global_completion \
--exclude environmental_shares_with_desinfo_counts \
--exclude path:models/advertising \
--exclude cas_de_mesinformation \
--exclude cas_affirmation_cible \
--exclude cas_categorie_visee \
--exclude publicites \
--exclude emission_jour \
--exclude pub_annonceur_emission_jour \
--exclude cas_theme \
--exclude path:models/curated

echo "apply dbt models to build the analytics tables (task_global_completion in 'analytics', environmental_shares_with_desinfo_counts in 'public')."
poetry run dbt run --full-refresh --target analytics \
--select task_global_completion \
--select environmental_shares_with_desinfo_counts

echo "apply advertising dbt models and the analytics tables built on them (publicites, cas_de_mesinformation), after the analytics tables they read (misinformation distance), then the daily aggregates built on them (pub_annonceur_emission_jour) and the themes of the cases (cas_theme)"
poetry run dbt run --full-refresh \
--select path:models/advertising \
--select cas_affirmation_cible \
--select cas_categorie_visee \
--select cas_de_mesinformation \
--select publicites \
--select pub_annonceur_emission_jour \
--select cas_theme

# incremental like models/dashboards: the days of the same window (DBT_LOOKBACK_WEEKS) and the days whose cases changed
# (see the model), after cas_de_mesinformation; rebuilt in full with "full-refresh" and on the first day of each month
echo "apply emission_jour (incremental)"
poetry run dbt run "${full_refresh[@]}" --vars "$lookback_vars" --select emission_jour

# the curated tables (models/curated) are built after this script by entrypoints/curated.sh, in its own Kestra task
