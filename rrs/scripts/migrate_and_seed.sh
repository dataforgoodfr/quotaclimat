#!/bin/sh
set -e

alembic -c rrs/alembic.ini upgrade head
python -m rrs.dictionary.upsert_subjects
python -m rrs.dictionary.upsert_dictionary
