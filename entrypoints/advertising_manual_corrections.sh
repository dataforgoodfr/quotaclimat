#!/bin/bash

set -e

echo "============================================"
echo "Starting advertising manual corrections"
echo "============================================"

python quotaclimat/data_ingestion/advertising/s04_consolidation/e02_manual_corrections/run.py

echo "============================================"
echo "Advertising manual corrections complete"
echo "============================================"
