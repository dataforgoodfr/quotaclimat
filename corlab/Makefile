SHELL := /bin/bash
tags ?=
local_port ?= 5433

# Loads corlab/.env (never committed) for every ansible/ssh command
LOAD_ENV = set -a; source ./.env; set +a

.PHONY: setup syntax check ansible migrate tunnel psql

setup: ## Install ansible and the collections the playbook needs
	@command -v sshpass >/dev/null || echo "WARNING: sshpass is required for password SSH (macOS: brew install hudochenkov/sshpass/sshpass, Debian/Ubuntu: apt install sshpass)"
	mise install
	mise exec -- ansible-galaxy collection install community.docker community.general ansible.posix

syntax: ## Check the playbook syntax
	$(LOAD_ENV); cd ansible; mise exec -- ansible-playbook playbook.yml --syntax-check

check: ## Dry run against the server
	$(LOAD_ENV); cd ansible; mise exec -- ansible-playbook playbook.yml --check --diff $(if $(tags),--tags $(tags),)

ansible: ## Deploy Traefik, Postgres and Metabase (make tags=metabase ansible for one role)
	$(LOAD_ENV); cd ansible; mise exec -- ansible-playbook playbook.yml $(if $(tags),--tags $(tags),)

migrate: ## Apply the alembic migrations of the repo to the barometre database
	$(LOAD_ENV); cd ansible; mise exec -- ansible-playbook playbook.yml --tags migrate

tunnel: ## Forward the server's Postgres to localhost:$(local_port) (Ctrl-C to stop)
	$(LOAD_ENV); ssh -p $${CORLAB_SSH_PORT:-2251} -N -L $(local_port):127.0.0.1:6436 $${CORLAB_SSH_USER:-ubuntu}@$$CORLAB_HOST_IP

psql: ## Open psql on the barometre database, on the server
	$(LOAD_ENV); ssh -p $${CORLAB_SSH_PORT:-2251} -t $${CORLAB_SSH_USER:-ubuntu}@$$CORLAB_HOST_IP \
		'docker exec -it postgres sh -c "psql -U \$$POSTGRES_USER -d \$$POSTGRES_DB"'
