#!/usr/bin/env bash
# Creates, in the Keycloak realm of the Metabase SSO, the groups the adapter maps to Metabase groups.
# Idempotent: a group that already exists is left alone. Names only, no members and no permissions.
#
# The group names are the lines of compose/sso/groups.txt (the default of SSO_GROUPS in ansible/playbook.yml),
# or SSO_GROUPS (comma separated) when it is set. They must equal the Metabase group names exactly.
#
# Usage (from infrastructure/, or `make keycloak-groups`, which loads .env.secrets):
#   bin/keycloak_create_groups.sh            # create the missing groups
#   bin/keycloak_create_groups.sh --dry-run  # only list what would be created
#
# Environment: KEYCLOAK_DOMAIN (or KEYCLOAK_URL, e.g. http://localhost:8080), KEYCLOAK_ADMIN_USER,
# KEYCLOAK_ADMIN_PASSWORD, KEYCLOAK_REALM (default metabase), optional SSO_GROUPS.
# If KEYCLOAK_ADMIN_ALLOWED_IPS is set, run it from an allowed address. Needs curl and jq.
set -euo pipefail

dry_run=false
[ "${1:-}" = "--dry-run" ] && dry_run=true

: "${KEYCLOAK_ADMIN_USER:?set KEYCLOAK_ADMIN_USER}" "${KEYCLOAK_ADMIN_PASSWORD:?set KEYCLOAK_ADMIN_PASSWORD}"
url="${KEYCLOAK_URL:-https://${KEYCLOAK_DOMAIN:?set KEYCLOAK_DOMAIN or KEYCLOAK_URL}}"
url="${url%/}"
realm="${KEYCLOAK_REALM:-metabase}"
root="$(cd "$(dirname "$0")/.." && pwd)"

groups=()
if [ -n "${SSO_GROUPS:-}" ]; then
  while IFS= read -r g; do groups+=("$g"); done < <(printf '%s' "$SSO_GROUPS" | tr ',' '\n')
else
  while IFS= read -r g; do groups+=("$g"); done < "$root/compose/sso/groups.txt"
fi

token=$(curl -fsS "$url/realms/master/protocol/openid-connect/token" \
  -d grant_type=password -d client_id=admin-cli \
  --data-urlencode "username=$KEYCLOAK_ADMIN_USER" --data-urlencode "password=$KEYCLOAK_ADMIN_PASSWORD" |
  jq -r .access_token)
[ -n "$token" ] && [ "$token" != null ] || { echo "Could not get an admin token from $url" >&2; exit 1; }

created=0 existing=0
for g in "${groups[@]}"; do
  g="$(printf '%s' "$g" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  [ -n "$g" ] || continue
  found=$(curl -fsS -G "$url/admin/realms/$realm/groups" -H "Authorization: Bearer $token" \
    --data-urlencode "search=$g" --data-urlencode exact=true |
    jq --arg n "$g" '[.[] | select(.name == $n)] | length')
  if [ "$found" -gt 0 ]; then
    echo "exists   $g"; existing=$((existing + 1))
  elif $dry_run; then
    echo "would create   $g"
  else
    curl -fsS -o /dev/null -X POST "$url/admin/realms/$realm/groups" -H "Authorization: Bearer $token" \
      -H 'Content-Type: application/json' -d "$(jq -n --arg n "$g" '{name: $n}')"
    echo "created  $g"; created=$((created + 1))
  fi
done
echo "realm $realm: $created created, $existing already existed"
