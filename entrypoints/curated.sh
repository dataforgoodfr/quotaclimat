#!/bin/bash
# Builds the curated tables (clean tables for the teams and their Claude, see my_dbt_project/models/curated/),
# after entrypoints/dbt.sh has built the tables they read, tests them, then pushes their documentation to
# Metabase (quotaclimat/metabase/sync_curated.py) when METABASE_URL is set, in each Metabase database of
# METABASE_CURATED_DATABASES (ids separated by spaces). Run by its own Kestra task, which may fail without stopping
# the pipeline.

echo "build the curated tables"
poetry run dbt run --full-refresh --select path:models/curated || exit 1

echo "test the curated tables"
poetry run dbt test --select path:models/curated
tests=$?

if [[ -n "$METABASE_URL" ]]; then
    echo "push the documentation of the curated tables to Metabase"
    databases=""
    for id in $METABASE_CURATED_DATABASES; do
        databases="$databases --database $id"
    done
    poetry run python -m quotaclimat.metabase.sync_curated --apply $databases || exit 1
else
    echo "METABASE_URL not set: documentation not pushed to Metabase"
fi

exit $tests
