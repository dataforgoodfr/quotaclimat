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
│   ├── rrs/                   # RRS database + serverless jobs + extended perimeter (Droit à l'info)
│   │   └── template/
│   │       ├── database.tf    # rrs + extended-perimeter databases, users/privileges, ACL
│   │       ├── s3.tf          # mediatree-extended-perimeter / misinformation-extended-perimeter buckets
│   │       ├── iam.tf         # rrs-ci application/policy/key (registry, jobs, object storage)
│   │       ├── secrets.tf     # Scaleway Secret Manager entries for job/migrate/read passwords, API keys
│   │       └── jobs.tf        # rrs-migrate/clustering/import-segments/import-cases job definitions
│   └── orchestrator/          # Kestra + GlitchTip Elastic Metal server (single shared instance)
│       ├── prod/
│       │   └── terragrunt.hcl
│       └── template/
│           ├── instance.tf    # Elastic Metal server (EM-A610R-NVMe), SSH keys, offer/OS
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

The orchestrator runs on a Scaleway **Elastic Metal** bare-metal server (EM-A610R-NVMe) hosting [Kestra](https://kestra.io) (workflow orchestration) and [GlitchTip](https://glitchtip.com) (error tracking), fronted by Traefik with automatic TLS. Docker data lives on the local NVMe (no attached block volume), and the firewall is UFW only (Elastic Metal has no Scaleway security group).

Unlike other targets, the orchestrator is a **single shared instance** — there is no dev/prod split. Kestra uses namespaces to separate environments, and GlitchTip uses projects.

### First-time deployment

```bash
# 1. Install tools
make setup

# 2. Set up credentials
cp .env.dist .env                    # Add SCW_ACCESS_KEY / SCW_SECRET_KEY
make sync-secrets                    # Or: cp .env.secrets.dist .env.secrets

# 3. Provision infrastructure (Elastic Metal server + databases)
make env=prod target=orchestrator tg-init
make env=prod target=orchestrator tg-plan
make env=prod target=orchestrator tg-apply

# 4. Create Ansible inventory from terraform output
make env=prod target=orchestrator tg-output
cd ansible && cp inventory.yml.j2 inventory.yml
# Edit inventory.yml: replace {{ instance_ip }} with the actual IP

# 5a. Allow-list the server IP on the managed PG (rdb-poc) ACL — Scaleway console
#     (rdb-poc's ACL is managed outside this repo). On migration, remove the old IP.

# 5b. Point DNS records to the server IP
#     traefik.<domain>, kestra.<domain>, glitchtip.<domain>

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

## RRS: extended perimeter (Droit à l'info)

On top of the regular `rrs` database, jobs and registry, the `rrs` target provisions a second, isolated set of resources for the "Droit à l'info" extended France perimeter (see the root [README's "Extended perimeter (Droit à l'info)" section](../README.md#extended-perimeter-droit-à-linfo) for the application-level `EXTENDED_PERIMETER` behaviour):
* An `extended-perimeter` Postgres database on the same `rrs` RDB instance (`live/rrs/template/database.tf`), with `admin` (full) and `job` (readwrite) privileges only, plus a dedicated readonly `rrs-read-<env>` user sharing its password with the `rrs-read-<env>` user on the `barometre` database — no migrate or metabase user access, unlike the `rrs` database.
* Two S3 buckets, `mediatree-extended-perimeter-<env>` and `misinformation-extended-perimeter-<env>` (`live/rrs/template/s3.tf`), read/write accessible via the existing `rrs-ci` IAM application/policy (`iam.tf`) — Scaleway IAM policies scope by project, not by bucket, so this key covers every bucket in the `rrs` project.
* The Kestra flow `infrastructure/kestra/flows/main_rrs_extendedperimeter.yaml` (dev only), which ingests Mediatree data to the `mediatree-extended-perimeter` bucket, runs keyword detection with `EXTENDED_PERIMETER: true` against the `extended-perimeter` database (using the admin user, since Alembic migrations there need DDL rights), then runs misinformation detection writing to the `misinformation-extended-perimeter` bucket.

To deploy/update just this target:
```bash
make env=dev target=rrs tg-plan
make env=dev target=rrs tg-apply
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

## Resource inventory

Every target below is deployed once per environment (`dev`/`prod`) from its `template/`, except `orchestrator` which is a single shared instance (`prod` only — `orchestrator/dev` has no `terragrunt.hcl`). List `make env=<env> target=<target> tg-state-list` for the authoritative, live state.

### `advertising` (`live/advertising/template/`)
| Resource | Address | Purpose |
|---|---|---|
| Scaleway Project | `scaleway_account_project.project` | Dedicated project for the target/environment |
| Container Namespace | `scaleway_container_namespace.container_namespace` | Namespace for the Label Studio container |
| Container | `scaleway_container.labelstudio_container` | Label Studio serverless container |
| RDB user | `scaleway_rdb_user.labelstudio_user` | Label Studio app DB user |
| RDB database | `scaleway_rdb_database.labelstudio_db` | Label Studio database |
| RDB privilege | `scaleway_rdb_privilege.labelstudio_policy` | Grants for `labelstudio_user` |
| RDB user | `scaleway_rdb_user.dgccrf_user` | DGCCRF-scoped DB user |
| RDB privilege | `scaleway_rdb_privilege.dgccrf_user_policy` | Grants for `dgccrf_user` |
| Terraform data (null resource) | `terraform_data.dgccrf_grants` | Additional grant provisioning for `dgccrf_user` |
| IAM Application | `scaleway_iam_application.project_application` | App identity for advertising detection jobs |
| IAM Policy | `scaleway_iam_policy.project_policy` | Permissions for `project_application` |
| IAM API key | `scaleway_iam_api_key.project_api_key` | Credentials for `project_application` |

### `barometre` (`live/barometre/template/`)
| Resource | Address | Purpose |
|---|---|---|
| Scaleway Project | `scaleway_account_project.project` | Dedicated project for the target/environment |
| RDB instance | `scaleway_rdb_instance.barometre_rdb` | Main Postgres instance (`rdb-poc`) backing the barometre pipeline |
| RDB database | `scaleway_rdb_database.barometre` | `barometre` database (keywords, program_metadata, stop_word, ...) |
| RDB user | `scaleway_rdb_user.database_admin` | Admin user for `barometre` (distinct from instance root admin) |
| RDB privilege | `scaleway_rdb_privilege.barometre_admin` | Full privileges for `database_admin` on `barometre` |
| RDB user | `scaleway_rdb_user.rrs_read` | Readonly user consumed cross-target by `rrs` (`import_segments`/`import_cases` jobs) |
| RDB privilege | `scaleway_rdb_privilege.rrs_read` | Readonly grant for `rrs_read` on `barometre` |
| RDB ACL | `scaleway_rdb_acl.public` | IP allow-list (Scaleway job CIDRs + dev/prod extra IPs) |
| Instance IP | `scaleway_instance_ip.gpu` | Flexible IP for the GPU instance |
| Instance server | `scaleway_instance_server.gpu` | GPU instance (Whisper transcription / ML workloads) |
| Object bucket | `scaleway_object_bucket.mediatree_videos` | Stores Mediatree video files |
| IAM Application | `scaleway_iam_application.mediatree_videos_application` | App identity scoped to the `mediatree-videos` bucket (project-wide in practice) |
| IAM Policy | `scaleway_iam_policy.mediatree_videos_policy` | Object storage read/write for `mediatree_videos_application` |
| IAM API key | `scaleway_iam_api_key.mediatree_videos_api_key` | Credentials for `mediatree_videos_application` |

### `rrs` (`live/rrs/template/`)
| Resource | Address | Purpose |
|---|---|---|
| Scaleway Project | `scaleway_account_project.project` | Dedicated project for the target/environment |
| Time sleep | `time_sleep.wait_for_project` | Works around Scaleway 403s right after project creation |
| RDB instance | `scaleway_rdb_instance.rrs_rdb` | Postgres instance backing `rrs` and `extended-perimeter` |
| RDB database | `scaleway_rdb_database.rrs` | Main RRS (misinformation clustering) database |
| RDB privilege | `scaleway_rdb_privilege.rrs_admin` | Full privileges for the instance admin user on `rrs` |
| RDB database | `scaleway_rdb_database.extended_perimeter` | Extended perimeter (Droit à l'info) database |
| RDB privilege | `scaleway_rdb_privilege.extended_perimeter_admin` | Full privileges for the instance admin user on `extended-perimeter` |
| RDB user | `scaleway_rdb_user.rrs_migrate_user` | Admin-capable user for running Alembic migrations on `rrs` |
| RDB privilege | `scaleway_rdb_privilege.rrs_migrate_user` | Full privileges for `rrs_migrate_user` on `rrs` |
| RDB user | `scaleway_rdb_user.rrs_job_user` | Readwrite user used by RRS jobs |
| RDB privilege | `scaleway_rdb_privilege.rrs_job_user` | Readwrite grant for `rrs_job_user` on `rrs` |
| RDB privilege | `scaleway_rdb_privilege.extended_perimeter_job_user` | Readwrite grant for `rrs_job_user` on `extended-perimeter` |
| RDB user | `scaleway_rdb_user.rrs_metabase_user` | Readonly user for Metabase on `rrs` |
| RDB privilege | `scaleway_rdb_privilege.rrs_metabase_user` | Readonly grant for `rrs_metabase_user` on `rrs` |
| RDB user | `scaleway_rdb_user.extended_perimeter_read` | Readonly `rrs-read-<env>` user, shares its password with `barometre`'s `rrs_read` |
| RDB privilege | `scaleway_rdb_privilege.extended_perimeter_read` | Readonly grant for `extended_perimeter_read` on `extended-perimeter` |
| RDB ACL | `scaleway_rdb_acl.public` | IP allow-list (Scaleway job CIDRs + dev/prod extra IPs) |
| IAM Application | `scaleway_iam_application.rrs_ci` | CI app identity for pushing images / triggering jobs |
| IAM Policy | `scaleway_iam_policy.rrs_ci` | Registry, serverless jobs and object storage permissions for `rrs_ci` |
| IAM API key | `scaleway_iam_api_key.rrs_ci` | Credentials for `rrs_ci` (CI `SCW_ACCESS_KEY`/`SCW_SECRET_KEY`) |
| Job definition | `scaleway_job_definition.rrs_migrate` | Runs `rrs/scripts/migrate_and_seed.sh` against `rrs` |
| Job definition | `scaleway_job_definition.rrs_clustering` | Misinformation clustering job |
| Job definition | `scaleway_job_definition.rrs_import_segments` | Imports keyword segments from `barometre` into `rrs` |
| Job definition | `scaleway_job_definition.rrs_import_cases` | Imports claims/cases from `barometre` into `rrs` |
| Registry namespace | `scaleway_registry_namespace.rrs` | Container registry for `rrs-base` images |
| Object bucket | `scaleway_object_bucket.mediatree_extended_perimeter` | Extended perimeter Mediatree ingestion data |
| Object bucket | `scaleway_object_bucket.misinformation_extended_perimeter` | Extended perimeter misinformation detection data |
| Secret + version | `scaleway_secret.postgres_migrate_password` / `..._version` | `rrs_migrate_user` password |
| Secret + version | `scaleway_secret.rrs_job_password` / `..._version` | `rrs_job_user` password |
| Secret + version | `scaleway_secret.mistral_api_key` / `..._version` | Mistral API key (clustering job) |
| Secret + version | `scaleway_secret.anthropic_api_key` / `..._version` | Anthropic API key (clustering job) |
| Secret + version | `scaleway_secret.barometre_rrs_read_password` / `..._version` | Shared password for `rrs-read-<env>` on both `barometre` and `extended-perimeter` |

### `orchestrator` (`live/orchestrator/template/`, `prod` only)
| Resource | Address | Purpose |
|---|---|---|
| Scaleway Project | `scaleway_account_project.project` | Dedicated project for the orchestrator |
| RDB user | `scaleway_rdb_user.kestra` | Kestra app DB user |
| RDB database | `scaleway_rdb_database.kestra` | Kestra database |
| RDB privilege | `scaleway_rdb_privilege.kestra` | Grants for `kestra` user |
| RDB user | `scaleway_rdb_user.glitchtip` | GlitchTip app DB user |
| RDB database | `scaleway_rdb_database.glitchtip` | GlitchTip database |
| RDB privilege | `scaleway_rdb_privilege.glitchtip` | Grants for `glitchtip` user |
| IAM SSH key | `scaleway_iam_ssh_key.paul_gabriel` | SSH access to the baremetal server |
| IAM SSH key | `scaleway_iam_ssh_key.gmguarino` | SSH access to the baremetal server |
| Baremetal server | `scaleway_baremetal_server.orchestrator` | Elastic Metal server (EM-A610R-NVMe) hosting Kestra + GlitchTip via Docker Compose/Ansible |
