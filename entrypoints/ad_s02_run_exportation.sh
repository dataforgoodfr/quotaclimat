#!/bin/bash

set pipefail
set -e

# Run migrations before starting the application
echo "Running migrations with alembic if exists"
poetry run alembic upgrade head

if [[ $? -eq 0 ]]; then
    echo "Command succeeded"
else
    echo "Command failed"
fi

# EXPORTATION_MODE: "run" (default) exports the new ads, "recompute_saved_chunks" recomputes the
# audio chunks of the ads already exported with the current fingerprinter parameters
# (see recompute_saved_chunks.py for MAX_ADS and CLEAN_OTHER_CHUNKS).
EXPORTATION_MODE="${EXPORTATION_MODE:-run}"

echo "============================================"
echo "Starting advertising exportation pipeline (EXPORTATION_MODE=${EXPORTATION_MODE})"
echo "============================================"

case "$EXPORTATION_MODE" in
    run)
        python quotaclimat/data_ingestion/advertising/s02_exportation/run.py
        ;;
    recompute_saved_chunks)
        python quotaclimat/data_ingestion/advertising/s02_exportation/recompute_saved_chunks.py
        ;;
    *)
        echo "Unknown EXPORTATION_MODE: ${EXPORTATION_MODE} (expected run or recompute_saved_chunks)"
        exit 1
        ;;
esac

echo "============================================"
echo "Advertising exportation pipeline complete"
echo "============================================"
