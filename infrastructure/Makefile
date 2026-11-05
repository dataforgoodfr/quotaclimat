env    ?= dev
target ?= advertising
tags   ?=

SHELL := $(shell which bash)
.SHELLFLAGS := -euo pipefail -c

# --- Setup ---

.PHONY: setup
setup:
	@$$SHELL ./bin/setup.sh

# --- Secrets ---

.PHONY: sync-secrets
sync-secrets:
	@source ./bin/lib/common.sh && refresh_vaultwarden_secrets

# --- Terraform / Terragrunt ---

define TG_CMD
	@source .env 2>/dev/null || true && \
	source .env.secrets 2>/dev/null || true && \
	source live/$(target)/$(env)/.env.secrets 2>/dev/null || true && \
	mise exec -- terragrunt $(1) --terragrunt-working-dir live/$(target)/$(env)
endef

.PHONY: tg-init tg-plan tg-apply tg-fmt tg-state-pull tg-state-list tg-state-rm tg-destroy tg-output

tg-init:
	$(call TG_CMD,init)

tg-plan:
	$(call TG_CMD,plan)

tg-apply:
	$(call TG_CMD,apply)

tg-fmt:
	$(call TG_CMD,hclfmt)

tg-state-pull:
	$(call TG_CMD,"state pull")

tg-state-list:
	$(call TG_CMD,"state list")

# Forgets resources in the state only: nothing is deleted in the cloud, the next
# apply would create them again. addr takes one or more resource addresses, e.g.
#   make target=orchestrator env=prod tg-state-rm addr="scaleway_object_bucket_policy.glitchtip scaleway_object_bucket.glitchtip"
tg-state-rm:
	@test -n "$(addr)" || { echo "usage: make target=<t> env=<e> tg-state-rm addr=\"<resource address> [...]\""; exit 1; }
	$(call TG_CMD,state rm $(addr))

tg-destroy:
	$(call TG_CMD,destroy)

# Prints all outputs as JSON, including sensitive values in clear text.
# With name=<output>, prints that one output raw (no quotes), e.g.
#   make target=orchestrator env=prod tg-output name=glitchtip_s3_secret_key
tg-output:
	$(call TG_CMD,output $(if $(name),-raw $(name),-json))

# --- Ansible ---

define ANSIBLE_CMD
	@source .env.secrets 2>/dev/null || true && \
	cd ansible && \
	mise exec -- ansible-playbook playbook.yml $(1)
endef

.PHONY: ansible

ansible:
ifdef tags
	$(call ANSIBLE_CMD,--tags $(tags))
else
	$(call ANSIBLE_CMD,)
endif
