#!/usr/bin/env bash
# Shared functions for Vaultwarden secret management.
# Sourced by Makefile targets.

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Vaultwarden collection ID for quotaclimat orchestrator secrets.
# TODO: Create this collection in Vaultwarden and update the ID.
VAULTWARDEN_COLLECTION_ID="${VAULTWARDEN_COLLECTION_ID:-CHANGEME}"
VAULTWARDEN_SERVER="${VAULTWARDEN_SERVER:-https://vaultwarden.services.dataforgood.fr}"

ENV_FILE="$INFRA_DIR/.env.secrets"

get_bw_session() {
  if [ -n "${BW_SESSION:-}" ]; then
    return
  fi

  bw config server "$VAULTWARDEN_SERVER" 2>/dev/null || true

  if ! bw login --check &>/dev/null; then
    echo "Logging in to Vaultwarden..."
    BW_SESSION=$(bw login --raw)
  else
    echo "Unlocking Vaultwarden vault..."
    BW_SESSION=$(bw unlock --raw)
  fi
  export BW_SESSION
}

refresh_vaultwarden_secrets() {
  if [ "$VAULTWARDEN_COLLECTION_ID" = "CHANGEME" ]; then
    echo "WARNING: VAULTWARDEN_COLLECTION_ID not set. Skipping secrets refresh."
    echo "Set it in your environment or update bin/lib/common.sh."
    return
  fi

  get_bw_session
  bw sync

  echo "Fetching secrets from Vaultwarden collection..."

  # Fetch all secure notes from the collection and write as KEY=VALUE
  bw list items --collectionid "$VAULTWARDEN_COLLECTION_ID" \
    | jq -r '.[] | select(.type == 2) | .notes' \
    > "$ENV_FILE"

  chmod 600 "$ENV_FILE"
  echo "Secrets written to $ENV_FILE"
}
