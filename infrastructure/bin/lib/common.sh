#!/usr/bin/env bash
# Shared functions for Vaultwarden secret management.
# Sourced by Makefile targets.

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

VAULTWARDEN_COLLECTION_ID="${VAULTWARDEN_COLLECTION_ID:-f964ef67-cb71-421e-90f1-fff328449b4b}"
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
  get_bw_session
  bw sync

  echo "Fetching secrets from Vaultwarden collection..."

  SECRETS=$(bw list items --collectionid "$VAULTWARDEN_COLLECTION_ID" --session "$BW_SESSION" \
    | jq -r '.[] | select(.type == 2) | .notes')

  if [ -z "$SECRETS" ]; then
    echo "ERROR: No secrets found in collection $VAULTWARDEN_COLLECTION_ID."
    echo "Check that you are a member of the Quotaclimat/Orchestrator collection."
    return 1
  fi

  echo -e '#!/usr/bin/env bash\n' > "$ENV_FILE"
  echo "$SECRETS" >> "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  echo "Secrets written to $ENV_FILE"
}
