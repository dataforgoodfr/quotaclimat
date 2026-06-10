# Safeguards Infrastructure
This folder contains the framework and infrastructure definition for the safeguards projects.

## The Stack
We deploy using `OpenTofu`, `Terragrunt`, and `Ansible`, all managed via `mise`. `make` is used as a wrapper for all commands, so it must also be installed before working with this repository.

Secrets are stored in the [DataForGood Vaultwarden Password Manager](https://vaultwarden.services.dataforgood.fr/) and can be fetched automatically via the `bw` CLI.

## Project Structure

```
infrastructure/
├── Makefile                   # Entry point for all commands
├── mise.toml                  # Tool version pins (opentofu, terragrunt, ansible)
├── .env                       # Scaleway credentials (not committed)
├── .env.secrets               # All secrets fetched from Vaultwarden (not committed)
├── .env.secrets.dist          # Template listing all required secrets
├── bin/
│   ├── setup.sh               # Installs all dependencies
│   └── lib/
│       └── common.sh          # Vaultwarden secret fetch functions
├── live/
│   ├── terragrunt.hcl         # Root config: backend, provider, terraform_binary
│   ├── advertising/           # Label Studio + advertising detection infra
│   ├── barometre/              # Barometre database infra
│   ├── rrs/                   # RRS database + serverless jobs
│   └── orchestrator/          # Kestra + GlitchTip VM (single shared instance)
│       ├── prod/
│       │   └── terragrunt.hcl
│       └── template/
│           ├── instance.tf    # VM, security group, SSH key, IP
│           ├── database.tf    # Kestra + GlitchTip databases on managed PG
│           ├── variables.tf
│           └── outputs.tf
├── compose/                   # Docker Compose stacks deployed by Ansible
│   ├── traefik/
│   ├── kestra/
│   └── glitchtip/
├── kestra/
│   └── flows/                 # Kestra flow definitions (uploaded by Ansible)
└── ansible/
    ├── ansible.cfg
    ├── inventory.yml.j2       # Template — fill with VM IP from terraform output
    ├── playbook.yml
    └── roles/
        ├── common/            # Docker, fail2ban, unattended-upgrades
        ├── traefik/           # Reverse proxy with auto-TLS
        ├── kestra/            # Orchestrator + secrets/KV provisioning + flow upload
        └── glitchtip/         # Error tracking + Redis
```

The root `live/terragrunt.hcl` generates the S3 backend and Scaleway provider configuration for every environment automatically, so there is no duplicated `backend.tf` or `provider.tf` to maintain. Each environment folder only needs a `terragrunt.hcl` with its specific input values.

## Installing dependencies

Prerequisites (install manually):
* `brew` — [brew.sh](https://brew.sh)
* `make` — `brew install make`
* `node` >= 18 — `brew install node` (required for `bw` CLI)

Then run:
```bash
make setup
```
This installs `mise`, `bw` (Bitwarden CLI), `jq`, `gum`, and all tool versions pinned in `mise.toml` (OpenTofu, Terragrunt, Ansible).

Make sure `mise` is activated in your shell profile:
```bash
eval "$(mise activate zsh)"   # Add to ~/.zshrc
```

## Setting up your workspace

### 1. Scaleway credentials

You need a Scaleway account and to be added as a member of the Quotaclimat organisation. Create an API key pair and add it to `.env`:

```bash
cp .env.dist .env
# Edit .env with your SCW_ACCESS_KEY and SCW_SECRET_KEY
```

### 2. Secrets from Vaultwarden

Secrets can be fetched automatically from Vaultwarden:

```bash
make sync-secrets
```

This fetches all secrets from the relevant Vaultwarden collection and writes them to `.env.secrets`. Both `tg-*` and `ansible` targets source this file automatically.

For manual setup, copy the template and fill values from Vaultwarden:
```bash
cp .env.secrets.dist .env.secrets
```

The secrets are available in the [DataForGood Vaultwarden](https://vaultwarden.services.dataforgood.fr/) under these collections:
* `Quotaclimat - Désinformation/` — existing Terraform secrets
* `Quotaclimat - Orchestrator/` — Kestra, GlitchTip, and flow secrets

## Make targets

### Secrets

| Command | Description |
|---------|-------------|
| `make sync-secrets` | Fetch all secrets from Vaultwarden into `.env.secrets` |

### Terraform / Terragrunt

All `tg-*` commands take `env` (default: `dev`) and `target` (default: `advertising`).

| Command | Description |
|---------|-------------|
| `make tg-init` | Initialise the target directory |
| `make tg-plan` | Show planned changes |
| `make tg-apply` | Apply changes |
| `make tg-destroy` | Destroy all resources |
| `make tg-fmt` | Format `.hcl` files |
| `make tg-state-list` | List resources in state |
| `make tg-state-pull` | Pull raw state |
| `make tg-output` | Show outputs |

Examples:
```bash
make env=prod target=orchestrator tg-init
make env=prod target=orchestrator tg-plan
make env=prod target=orchestrator tg-apply
```

### Ansible

| Command | Description |
|---------|-------------|
| `make ansible` | Run all roles |
| `make tags=kestra ansible` | Run only the kestra role |
| `make tags=traefik,kestra ansible` | Run specific roles |

Available tags: `common`, `traefik`, `kestra`, `glitchtip`.

Before running Ansible for the first time, create the inventory file from the template:
```bash
cd ansible
cp inventory.yml.j2 inventory.yml
# Replace {{ instance_ip }} with the VM IP from: make env=prod target=orchestrator tg-output
```

### Setup

| Command | Description |
|---------|-------------|
| `make setup` | Install all dependencies (mise, bw, jq, gum, opentofu, terragrunt, ansible) |

## Orchestrator deployment

The orchestrator VM hosts [Kestra](https://kestra.io) (workflow orchestration) and [GlitchTip](https://glitchtip.com) (error tracking), fronted by Traefik with automatic TLS.

Unlike other targets, the orchestrator is a **single shared instance** — there is no dev/prod split. Kestra uses namespaces to separate environments, and GlitchTip uses projects.

### First-time deployment

```bash
# 1. Install tools
make setup

# 2. Set up credentials
cp .env.dist .env                    # Add SCW_ACCESS_KEY / SCW_SECRET_KEY
make sync-secrets                    # Or: cp .env.secrets.dist .env.secrets

# 3. Provision infrastructure (VM + databases)
make env=prod target=orchestrator tg-init
make env=prod target=orchestrator tg-plan
make env=prod target=orchestrator tg-apply

# 4. Create Ansible inventory from terraform output
make env=prod target=orchestrator tg-output
cd ansible && cp inventory.yml.j2 inventory.yml
# Edit inventory.yml: replace {{ instance_ip }} with the actual IP

# 5. Point DNS records to the VM IP
#    traefik.<domain>, kestra.<domain>, glitchtip.<domain>

# 6. Deploy services
cd .. && make ansible
```

### Updating a single service

```bash
make tags=kestra ansible     # Redeploy Kestra + re-upload flows + sync secrets
make tags=glitchtip ansible  # Redeploy GlitchTip only
```

### Kestra flows

Flow definitions live in `kestra/flows/`. The Ansible kestra role automatically:
1. Uploads all flow YAML files to Kestra via its API
2. Provisions secrets referenced by `{{ secret('...') }}` in flows
3. Provisions KV store entries referenced by `{{ kv('...') }}` in flows

To add or update a flow, edit the YAML in `kestra/flows/` and run:
```bash
make tags=kestra ansible
```

## A note on passwords
When deploying for the first time you may need to create passwords and tokens. Use the following to generate a secure password:
```bash
LC_ALL=C tr -dc 'A-Za-z0-9!@#$%^&*' < /dev/urandom | head -c 32; echo
```
To generate a Label Studio token (40-character alphanumeric string):
```bash
openssl rand -base64 30 | tr -dc 'A-Za-z0-9' | head -c 40; echo
```
To generate a Traefik basicAuth password:
```bash
htpasswd -nB admin | sed -e 's/\$/\$\$/g'
```
