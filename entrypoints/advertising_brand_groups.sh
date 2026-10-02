#!/bin/bash

set -e

echo "============================================"
echo "Starting advertising brand groups proposals"
echo "============================================"

python quotaclimat/data_ingestion/advertising/s04_brand_groups/run.py

echo "============================================"
echo "Advertising brand groups proposals complete"
echo "============================================"
