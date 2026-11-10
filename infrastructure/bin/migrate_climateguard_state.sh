#!/usr/bin/env bash
# One-off migration of the climateguard infrastructure states into
# live/misinformation/<env> (one state per environment, countries as for_each).
#
# climateguard had, per environment, one state for "common" and one per
# country (module.<name>.<resource>), plus an "environments" state holding the
# Scaleway projects. This script:
#   1. pulls those states read-only through climateguard's own Makefile;
#   2. merges them with jq into a single state:
#        module.<any>.<type>.<name> (country state)  ->  <type>.<name>["<country>"]
#        module.common.<type>.<name>                 ->  <type>.<name>
#        scaleway_account_project.project_climatesafeguards_<env> -> scaleway_account_project.project
#      Data sources are dropped (re-read at plan), every resource is moved to
#      the default scaleway/scaleway provider (climateguard used aliased,
#      per-project providers), and the climate_safeguards_<env>_tofu IAM
#      applications are left out (no longer used: deploys use the shared key);
#   3. checks the resource count, then, only with --push, pushes the state to
#      quotaclimat-terraform-states/misinformation/<env>/terraform.tfstate.
#
# The climateguard states are not modified: they stay as a backup.
# After --push, run `make target=misinformation env=<env> tg-plan`: expected
# result is no change (see infrastructure/Readme.md, "Misinformation").
#
# Usage (from infrastructure/):
#   bin/migrate_climateguard_state.sh <dev|prod> <path to climateguard/infrastructure> [--push]

set -euo pipefail

env="${1:?usage: $0 <dev|prod> <climateguard infrastructure dir> [--push]}"
old_infra="${2:?usage: $0 <dev|prod> <climateguard infrastructure dir> [--push]}"
push="${3:-}"
infra_dir="$(cd "$(dirname "$0")/.." && pwd)"

case "$env" in dev | prod) ;; *) echo "env must be dev or prod" >&2; exit 1 ;; esac

work="$(mktemp -d)"
chmod 700 "$work"
echo "Working directory (contains secrets, delete it afterwards): $work"

# --- 1. Pull the climateguard states (read-only) ---
countries=()
for dir in "$old_infra/live/climate/$env"/*/; do
  name="$(basename "$dir")"
  [ "$name" = common ] || countries+=("$name")
  (cd "$old_infra" && make -s subject=climate env="$env" country="$name" tofu-state-pull) > "$work/$name.tfstate"
done
(cd "$old_infra" && make -s environments=1 tofu-state-pull) > "$work/environments.tfstate"
echo "Pulled: common ${countries[*]} environments"

# --- 2. Merge ---
provider='provider["registry.opentofu.org/scaleway/scaleway"]'

# Managed resources of one climateguard state, without module, with the default
# provider, and with index_key set to $key when $key is not empty.
to_root() {
  jq --arg key "$2" --arg provider "$provider" '
    [.resources[]
     | select(.mode == "managed")
     | del(.module)
     | .provider = $provider
     | .instances |= map(
         (if $key != "" then .index_key = $key else . end)
         | if .dependencies then .dependencies |= map(sub("^module\\.[^.]+\\."; "")) else . end
       )]' "$1"
}

{
  to_root "$work/common.tfstate" ""
  for c in "${countries[@]}"; do to_root "$work/$c.tfstate" "$c"; done
  jq --arg env "$env" --arg provider "$provider" '
    [.resources[]
     | select(.mode == "managed" and .name == "project_climatesafeguards_\($env)")
     | .name = "project"
     | .provider = $provider]' "$work/environments.tfstate"
} | jq -s --arg lineage "$(uuidgen | tr '[:upper:]' '[:lower:]')" '
  add
  # for_each resources: one entry per type.name holding every country instance
  | group_by(.type + "." + .name)
  | map(.[0] + {instances: (map(.instances) | add)})
  | {version: 4, terraform_version: "1.12.0", serial: 1, lineage: $lineage, outputs: {}, resources: ., check_results: null}
' > "$work/new.tfstate"
chmod 600 "$work"/*.tfstate

# --- 3. Check ---
expected=$(( $(jq '[.resources[] | select(.mode == "managed") | .instances | length] | add' "$work/common.tfstate") + 1 ))
for c in "${countries[@]}"; do
  expected=$(( expected + $(jq '[.resources[] | select(.mode == "managed") | .instances | length] | add' "$work/$c.tfstate") ))
done
actual=$(jq '[.resources[].instances | length] | add' "$work/new.tfstate")
echo "Resource instances: expected $expected, merged $actual"
[ "$expected" = "$actual" ] || { echo "Count mismatch, not pushing." >&2; exit 1; }

echo "Addresses in the new state:"
jq -r '.resources[] | . as $r | .instances[] | "  \($r.type).\($r.name)\(if .index_key then "[\"\(.index_key)\"]" else "" end)"' "$work/new.tfstate"

if [ "$push" != "--push" ]; then
  echo "Dry run: re-run with --push to write it to misinformation/$env."
  exit 0
fi

cd "$infra_dir"
# Same environment as the Makefile's TG_CMD
set +u
source .env 2>/dev/null || true
source .env.secrets 2>/dev/null || true
source "live/misinformation/$env/.env.secrets" 2>/dev/null || true
set -u
mise exec -- terragrunt init --terragrunt-working-dir "live/misinformation/$env"
mise exec -- terragrunt state push "$work/new.tfstate" --terragrunt-working-dir "live/misinformation/$env"
echo "Pushed. Now run: make target=misinformation env=$env tg-plan"
