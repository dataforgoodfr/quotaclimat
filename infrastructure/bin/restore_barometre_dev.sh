#!/bin/bash
# Restores a local backup file into the dev barometre database.
# Usage: ./restore_barometre_dev.sh <path-to-dump-file>
#
# Credentials are sourced automatically from .env and live/barometre/dev/.env.secrets.
# The dev instance endpoint is resolved via the Scaleway CLI.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_ROOT="$(dirname "$SCRIPT_DIR")"

BACKUP_FILE="${1:?Usage: $0 <path-to-dump-file>}"

if [ ! -f "$BACKUP_FILE" ]; then
  echo "Error: backup file not found: $BACKUP_FILE"
  exit 1
fi

BACKUP_FILE="$(cd "$(dirname "$BACKUP_FILE")" && pwd)/$(basename "$BACKUP_FILE")"
BACKUP_DIR="$(dirname "$BACKUP_FILE")"

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

echo "WARNING: This will drop and recreate the public schema in the dev database ($DEV_HOST)."
read -r -p "Continue? [y/N] " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
  echo "Aborted."
  exit 0
fi

echo "Restoring $(basename "$BACKUP_FILE") to dev database ($DEV_HOST) ..."

docker run --rm \
  -v "$BACKUP_DIR:/backups" \
  postgres:15 bash -c \
  "PGPASSWORD='${DEV_POSTGRES_PASSWORD}' pg_restore \
    -h ${DEV_HOST} \
    -p ${DEV_PORT} \
    -U ${DEV_POSTGRES_USER} \
    -d barometre \
    --no-owner --no-acl \
    --clean --if-exists \
    /backups/$(basename "$BACKUP_FILE")"

echo "Restore complete."
