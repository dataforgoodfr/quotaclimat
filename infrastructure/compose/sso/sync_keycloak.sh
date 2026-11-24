#!/usr/bin/env bash
# Pushes the secrets of .env.secrets into the running Keycloak. Run by the sso role inside the Keycloak container
# (docker exec -i -e ... keycloak bash -s), after Keycloak is healthy.
#
# Why: the realm file (realm.json.j2) is imported on Keycloak's FIRST start only. A secret that was empty or
# different at that moment stays so, and redeploying does not change it. This brings Keycloak back in line with the
# secrets at every deploy: idempotent, and it only sets the values below (nothing else of the realm is touched).
#
# Environment (never in the arguments, so they stay out of process lists and logs): KC_ADMIN_USER,
# KC_ADMIN_PASSWORD, KC_REALM, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, SSO_CLIENT_SECRET, SSO_ADMIN_CLIENT_SECRET.
set -euo pipefail

kcadm=/opt/keycloak/bin/kcadm.sh

# The server listens on localhost inside the container: no Traefik, no admin IP allow-list in the way
"$kcadm" config credentials --server http://localhost:8080 --realm master \
  --user "$KC_ADMIN_USER" --password "$KC_ADMIN_PASSWORD"

# Google identity provider
"$kcadm" update identity-provider/instances/google -r "$KC_REALM" \
  -s "config.clientId=$GOOGLE_CLIENT_ID" -s "config.clientSecret=$GOOGLE_CLIENT_SECRET"
echo "google identity provider: client id and secret set"

# Secrets of the two clients of the adapter
set_client_secret() {
  local client_id="$1" secret="$2" id
  id=$("$kcadm" get clients -r "$KC_REALM" -q "clientId=$client_id" --fields id --format csv --noquotes)
  [ -n "$id" ] || { echo "client $client_id not found in realm $KC_REALM" >&2; exit 1; }
  "$kcadm" update "clients/$id" -r "$KC_REALM" -s "secret=$secret"
  echo "client $client_id: secret set"
}
set_client_secret metabase-sso "$SSO_CLIENT_SECRET"
set_client_secret metabase-sso-admin "$SSO_ADMIN_CLIENT_SECRET"
