#!/bin/bash
# Migrates schema and data from the prod barometre instance to the dev instance.
# Idempotent: skips if the target database already contains tables.
#
# Credentials are sourced automatically from .env and live/barometre/dev/.env.secrets.
# The dev instance endpoint is resolved via the Scaleway CLI.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_ROOT="$(dirname "$SCRIPT_DIR")"

source "$INFRA_ROOT/.env"
source "$INFRA_ROOT/live/barometre/dev/.env.secrets"

: "${PROD_POSTGRES_HOST:?PROD_POSTGRES_HOST is required}"
: "${PROD_POSTGRES_PORT:=5432}"
: "${PROD_POSTGRES_USER:?PROD_POSTGRES_USER is required}"
: "${PROD_POSTGRES_PASSWORD:?PROD_POSTGRES_PASSWORD is required}"
: "${DEV_POSTGRES_USER:?DEV_POSTGRES_USER is required}"
: "${DEV_POSTGRES_PASSWORD:?DEV_POSTGRES_PASSWORD is required}"

# Resolve the dev project ID by name.
DEV_PROJECT_ID=$(scw account project list -o json | \
  jq -r '.[] | select(.name == "barometre-dev") | .id')

if [ -z "$DEV_PROJECT_ID" ]; then
  echo "Error: project 'barometre-dev' not found. Run tg-apply first."
  exit 1
fi

# Resolve the dev instance endpoint.
DEV_INSTANCE=$(scw rdb instance list \
  project-id="$DEV_PROJECT_ID" \
  region=fr-par \
  -o json | jq -r '.[] | select(.name == "rdb-barometre-dev")')

DEV_HOST=$(echo "$DEV_INSTANCE" | jq -r '.endpoints[0].ip')
DEV_PORT=$(echo "$DEV_INSTANCE" | jq -r '.endpoints[0].port')

if [ -z "$DEV_HOST" ] || [ "$DEV_HOST" = "null" ]; then
  echo "Error: could not resolve dev instance endpoint. Is the ACL configured?"
  exit 1
fi

# Idempotency check: skip if tables already exist in the target.
TABLE_COUNT=$(PGPASSWORD="$DEV_POSTGRES_PASSWORD" psql \
  -h "$DEV_HOST" -p "$DEV_PORT" -U "$DEV_POSTGRES_USER" -d barometre -tAc \
  "SELECT count(*) FROM information_schema.tables \
   WHERE table_schema NOT IN ('pg_catalog', 'information_schema');")

if [ "$TABLE_COUNT" -gt 0 ]; then
  echo "Target already has $TABLE_COUNT tables — skipping migration."
  exit 0
fi

echo "Migrating schema and data from prod ($PROD_POSTGRES_HOST) to dev ($DEV_HOST)..."

docker run --rm postgres:15 bash -c \
  "PGPASSWORD='${PROD_POSTGRES_PASSWORD}' pg_dump \
    -h ${PROD_POSTGRES_HOST} \
    -p ${PROD_POSTGRES_PORT} \
    -U ${PROD_POSTGRES_USER} \
    -d barometre \
    --no-owner --no-acl | \
   PGPASSWORD='${DEV_POSTGRES_PASSWORD}' psql \
    -h ${DEV_HOST} \
    -p ${DEV_PORT} \
    -U ${DEV_POSTGRES_USER} \
    -d barometre"

echo "Migration complete."
