#!/bin/bash
# Dumps the dev barometre database to a local file.
# Output: ./backups/barometre_dev_<timestamp>.dump (custom pg_dump format)
#
# Credentials are sourced automatically from .env and live/barometre/dev/.env.secrets.
# The dev instance endpoint is resolved via the Scaleway CLI.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_ROOT="$(dirname "$SCRIPT_DIR")"
BACKUP_DIR="$INFRA_ROOT/backups"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="$BACKUP_DIR/barometre_dev_${TIMESTAMP}.dump"

source "$INFRA_ROOT/.env"
source "$INFRA_ROOT/live/barometre/dev/.env.secrets"

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

mkdir -p "$BACKUP_DIR"

echo "Backing up dev database ($DEV_HOST) to $BACKUP_FILE ..."

docker run --rm \
  -v "$BACKUP_DIR:/backups" \
  postgres:15 bash -c \
  "PGPASSWORD='${DEV_POSTGRES_PASSWORD}' pg_dump \
    -h ${DEV_HOST} \
    -p ${DEV_PORT} \
    -U ${DEV_POSTGRES_USER} \
    -d barometre \
    --no-owner --no-acl \
    -Fc \
    -f /backups/barometre_dev_${TIMESTAMP}.dump"

echo "Backup complete: $BACKUP_FILE"
