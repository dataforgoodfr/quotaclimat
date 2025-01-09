#!/bin/bash

# Run migrations before starting the application
echo "Running migrations with alembic if exists"
poetry run alembic upgrade head

if [[ $? -eq 0 ]]; then
    echo "Command succeeded"
else
    echo "Command failed"
fi

echo "starting stop_word import app"
python quotaclimat/data_processing/mediatree/stop_word/main.py