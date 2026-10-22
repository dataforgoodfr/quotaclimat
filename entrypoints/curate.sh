#!/bin/bash
# Builds the curate tables (clean tables for the teams and their Claude, see my_dbt_project/models/curate/),
# after entrypoints/dbt.sh has built the tables they read, tests them, then pushes their documentation to
# Metabase (quotaclimat/metabase/sync_curate.py) when METABASE_URL is set, in each Metabase database of
# METABASE_CURATE_DATABASES (ids separated by spaces). Run by its own Kestra task, which may fail without stopping
# the pipeline.

echo "build the curate tables"
poetry run dbt run --full-refresh --select path:models/curate || exit 1

echo "test the curate tables"
poetry run dbt test --select path:models/curate
tests=$?

if [[ -n "$METABASE_URL" ]]; then
    echo "push the documentation of the curate tables to Metabase"
    databases=""
    for id in $METABASE_CURATE_DATABASES; do
        databases="$databases --database $id"
    done
    poetry run python -m quotaclimat.metabase.sync_curate --apply $databases || exit 1
else
    echo "METABASE_URL not set: documentation not pushed to Metabase"
fi

exit $tests
