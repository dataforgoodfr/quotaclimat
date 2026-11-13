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
│   ├── migrate_climateguard_state.sh  # One-off: climateguard states → live/misinformation
│   └── lib/
│       └── common.sh          # Vaultwarden secret fetch functions
├── live/
│   ├── terragrunt.hcl         # Root config: backend, provider, terraform_binary
│   ├── advertising/           # Label Studio + advertising detection infra
│   ├── barometre/              # Barometre database infra
│   ├── misinformation/        # Label Studio + label-misinformation job per country (climate-safeguards-<env>)
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
│           ├── s3.tf          # GlitchTip S3 bucket, scoped IAM application + API key
│           ├── variables.tf
│           └── outputs.tf
├── compose/                   # Docker Compose stacks deployed by Ansible
│   ├── traefik/
│   ├── kestra/
│   ├── glitchtip/
│   └── windmill/
├── kestra/
│   └── flows/                 # Kestra flow definitions (uploaded by Ansible)
├── windmill/                  # Windmill copy of the Kestra flows (wmill project, pushed by Ansible)
│   ├── wmill.yaml             # Synced folders (sync push deletes what is missing in them)
│   └── f/                     # quotaclimat, quotaclimat_dev, rrs, rrs_dev, shared (see docs/windmill_flows.md)
└── ansible/
    ├── ansible.cfg
    ├── inventory.yml.j2       # Template — fill with VM IP from terraform output
    ├── playbook.yml
    └── roles/
        ├── common/            # Docker, fail2ban, unattended-upgrades
        ├── traefik/           # Reverse proxy with auto-TLS
        ├── kestra/            # Orchestrator + secrets/KV provisioning + flow upload
        ├── glitchtip/         # Error tracking + Redis
        └── windmill/          # Windmill server + workers (Kestra alternative) + secrets sync + flow push
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
| `make tg-state-rm addr="<address> [...]"` | Remove resources from the state only (nothing is deleted in the cloud) |
| `make tg-output` | Show outputs as JSON, sensitive values in clear text (`name=<output>` prints one raw) |

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

Available tags: `common`, `traefik`, `kestra`, `glitchtip`, `windmill`.

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
#     traefik.<domain>, kestra.<domain>, glitchtip.<domain>, windmill.<domain>

# 6. Deploy services
cd .. && make ansible
```

### Updating a single service

```bash
make tags=kestra ansible     # Redeploy Kestra + re-upload flows + sync secrets
make tags=glitchtip ansible  # Redeploy GlitchTip only
make tags=windmill ansible   # Redeploy Windmill + sync secrets + push flows
```

[Windmill](https://www.windmill.dev/docs/advanced/self_host) (Kestra alternative) runs as a server plus default, native and `dbt` workers (`compose/windmill/docker-compose.yml.j2`, role `windmill`), on its own `windmill` database on the managed Postgres. Traefik routes `WINDMILL_DOMAIN` to the server (port 8000) with automatic TLS. Set `WINDMILL_DOMAIN`, `WINDMILL_DB_USER`, `WINDMILL_DB_PASSWORD`, `WINDMILL_DB_NAME` in `.env.secrets` and `TF_VAR_postgres_password_windmill` (same value as `WINDMILL_DB_PASSWORD`) for OpenTofu. First login is `admin@windmill.dev` / `changeme`: change it right away.

The Windmill copy of the Kestra flows (`windmill/`, no schedules yet: Kestra still runs production) is deployed by the same role, after Windmill starts: it writes every entry of `kestra_secrets`/`kestra_secrets_dev` as the secret variable `f/secrets/<NAME>` (API, only what changed), then pushes `windmill/` with `wmill sync push`, which makes the synced Windmill folders match the repository, deletions included: create and change flows in `windmill/`, never in the Windmill UI. Before the first deploy, create the workspace in the UI and an API token of a superadmin (account settings → Tokens), then set in `.env.secrets`: `WINDMILL_WORKSPACE` (the workspace **id**, e.g. `quotaclimat`) and `WINDMILL_TOKEN` (superadmin: it also writes the instance SMTP settings).

Invitation emails go through the same Brevo account as GlitchTip (`BREVO_SMTP_LOGIN`, `BREVO_SMTP_KEY`, `BREVO_SMTP_HOST`): the role writes them as the `smtp_settings` instance setting (`ansible/roles/windmill/tasks/smtp.yml`, same fields as Instance settings → SMTP in the UI, port left to Windmill's default), with `WINDMILL_FROM_EMAIL` as sender (default `windmill@quotaclimat.org`, on the domain verified in Brevo). Skipped when `BREVO_SMTP_LOGIN`/`BREVO_SMTP_KEY` are empty. Windmill masks the stored password, so the setting is rewritten only when another field changes: after changing only `BREVO_SMTP_KEY`, run `WINDMILL_SMTP_FORCE=true make tags=windmill ansible` (or update it in the UI). The workers mount the Docker socket to run the flows' containers, like Kestra. Details (conversion from Kestra, container runner, dbt worker, adding a flow or a secret): [`docs/windmill_flows.md`](../docs/windmill_flows.md).

The `windmill` user creates and owns its own database (`live/orchestrator/template/grants.tf`, `terraform_data.windmill_database`): a database created by Scaleway's API would be owned by `_rdb_superadmin`, and nobody else could grant the Windmill roles `USAGE` on its `public` schema. The managed Postgres has no superuser, so `windmill_admin` is created without `BYPASSRLS` and relies on Windmill's row-level-security migration: keep the image (`windmill_image`, `ansible/roles/windmill/defaults/main.yml`) recent. Creating the database and roles needs `CREATEDB`/`CREATEROLE`, so `windmill` is a Scaleway admin only during the bootstrap:

```bash
TF_VAR_windmill_bootstrap=true make env=prod target=orchestrator tg-apply   # windmill is admin, creates its database and roles
make env=prod target=orchestrator tg-apply                                  # removes admin; windmill stays owner of its database
```

Before removing Windmill, apply once with `TF_VAR_windmill_bootstrap=true`: the destroy step (`DROP DATABASE windmill`, `DROP ROLE windmill_admin, windmill_user`) needs `CREATEROLE` to drop the roles.

GlitchTip stores files and archived events/logs (DuckDB cold storage) in S3. After `make env=prod target=orchestrator tg-apply`, copy the `glitchtip_s3_*` outputs into `GLITCHTIP_S3_ACCESS_KEY`, `GLITCHTIP_S3_SECRET_KEY` and `GLITCHTIP_S3_BUCKET` in `.env.secrets` (rendered by `compose/glitchtip/.env.s3.j2`). Invitation emails go through [Brevo](https://www.brevo.com) SMTP (`compose/glitchtip/.env.email.j2`, loaded after `.env`). Verify the `quotaclimat.org` domain in Brevo (DKIM/DMARC), create an SMTP key (SMTP & API), then set in `.env.secrets`: `BREVO_SMTP_LOGIN` (Brevo login email), `BREVO_SMTP_KEY`, and optionally `GLITCHTIP_FROM_EMAIL` (default `glitchtip@quotaclimat.org`), `BREVO_SMTP_HOST` (default `smtp-relay.brevo.com`), `BREVO_SMTP_PORT` (default `587`). Without `BREVO_SMTP_LOGIN`/`BREVO_SMTP_KEY`, emails are only printed in the container logs (`consolemail://`). Redeploy with `make tags=glitchtip ansible`. If sending fails, check the Brevo logs and try port 465 with `smtp+ssl` in the template.
The managed Postgres sets `max_locks_per_transaction = 512` for GlitchTip (`live/barometre/template/database.tf`).

### Kestra flows

Flow definitions live in `kestra/flows/`. The Ansible kestra role automatically:
1. Copies all flow YAML files to `/opt/services/kestra/flows` on the server, bind-mounted as `/app/flows` and watched by Kestra (`micronaut.io.watch`), and removes the files of flows deleted from `kestra/flows/` (the directory itself is never deleted: the container would stay on the old, empty one and lose every flow)
2. Restarts the `kestra` container on every run so it reloads the flows
3. Provisions secrets referenced by `{{ secret('...') }}` in flows
4. Provisions KV store entries referenced by `{{ kv('...') }}` in flows

To add or update a flow, edit the YAML in `kestra/flows/` and run:
```bash
make tags=kestra ansible
```

### Failure alerts (Mattermost)

The `main-pipeline` flow posts a message in a Mattermost channel when a step fails (`errors:` block, plain HTTP `Request` task posting to the webhook, with links to the execution and to the failed subflow). The channel is the one of an [incoming webhook](https://developers.mattermost.com/integrate/webhooks/incoming/) (Integrations > Incoming Webhooks), whose URL is listed in the `MATTERMOST_WEBHOOK_URLS` secret (`.env.secrets`, provisioned as a Kestra secret by the `kestra` role). Several webhooks (channels or servers) can be given, comma-separated without spaces, and each gets the message. To change the channels, update the secret, then `make tags=kestra ansible`.

`metabase_sync` has `allowFailure: true`: its failure marks the run as WARNING and sends no alert.

To test the alert, run the dev `main-pipeline` (namespace `quotaclimat-dev`) by hand with `test_alert=true` (see `docs/kestra_flows.md`).

### Per-country secret (`I18N_COUNTRY_SECRETS`)

The `barometre-i18n` flow runs the misinformation job for every country on the VM. Instead of one Kestra secret per country, the per-country credentials are in a single secret, `I18N_COUNTRY_SECRETS`: a **single-line JSON object** keyed by the flow's country codes (`bel`, `bel-fla`, `deu`, `esp`, `pol`), each value holding four strings:

```json
{"bel":{"bucket_access":"...","bucket_secret":"...","label_studio_token":"...","label_studio_url":"https://..."},"bel-fla":{...},"deu":{...},"esp":{...},"pol":{...}}
```

| Key | Source (`misinformation` target, project `climate-safeguards-prod`) |
|---|---|
| `bucket_access` | Secret Manager `access-key-<country>` (path `/<country>`, e.g. `belgium-flanders` for `bel-fla`) |
| `bucket_secret` | Secret Manager `secret-key-<country>` |
| `label_studio_url` | `make env=prod target=misinformation tg-output name=labelstudio_urls` (`https://<namespace>-labelstudio-<country>-<env>.functions.fnc.fr-par.scw.cloud`) |
| `label_studio_token` | Secret Manager `labelstudio-<country>-token` (`TF_VAR_labelstudio_user_token_<country>` in `live/misinformation/prod/.env.secrets`) |

Build it once with `jq -n -c --arg ...` (so values are escaped), store it as one entry in the `Quotaclimat - Orchestrator/` collection, then `make sync-secrets` and `make tags=kestra ansible`. Check each code with `echo "$I18N_COUNTRY_SECRETS" | jq -e '.bel.bucket_access'` before deploying. Adding a country adds one key here, no playbook change. The key is already declared in `kestra_secrets` (`ansible/playbook.yml`); add `export I18N_COUNTRY_SECRETS=` to `.env.secrets.dist`.

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

## Misinformation (climate safeguards)

The `misinformation` target deploys the infrastructure of the misinformation labelling jobs (code in `detect_misinformation/jobs/label_misinformation`, formerly the `climateguard` repository), in the Scaleway project `climate-safeguards-<env>`. For **every country** of the `countries` map of `live/misinformation/<env>/terragrunt.hcl` it creates:
* a Label Studio serverless container `labelstudio-<country>-<env>`, whose database `labelstudio-climate-<country>-<env>-db` is on the barometre RDB instance (`rdb-poc`);
* an output bucket `safeguards-climate-<country>-<env>`, an IAM application `app-climate-<country>-<env>` with its API key;
* Secret Manager entries `access-key-<country>`, `secret-key-<country>`, `labelstudio-<country>-token` (path `/<country>`), read by Kestra's `barometre-i18n` flow (see `I18N_COUNTRY_SECRETS` above);
* the Scaleway job `label-misinformation-<country>-<env>` (image `rg.fr-par.scw.cloud/misinformation/label-misinformation:<image_tag>`). Dev jobs only run on Jan 1st: they are launched by hand.

Shared by all countries of an environment: the container namespace `climate-safeguards-containers-<env>` and the `/common` secrets (Mediatree password, OpenAI/Mistral keys, barometre password).

Countries (dev and prod): `belgium`, `belgium-flanders`, `germany`, `poland`, `spain`. France is not part of this target: its labelling runs from the barometre pipeline.

**Adding a country**: add an entry to `countries` (model, Label Studio project ids, cron) and its three secrets `TF_VAR_postgres_password_labelstudio_<country>`, `TF_VAR_labelstudio_admin_password_<country>`, `TF_VAR_labelstudio_user_token_<country>` (with `-` replaced by `_`) to `live/misinformation/<env>/.env.secrets`, its `.env.secrets.dist` and the `live/misinformation/<env>` Vaultwarden item, then `make env=<env> target=misinformation tg-plan`. Country names end up in resource names: never rename an existing one, it would recreate its resources.

```bash
make env=dev target=misinformation tg-plan
make env=dev target=misinformation tg-apply
```

### Migration from climateguard

Until this target, these resources were managed from the `climateguard` repository (plain OpenTofu, one state per country in the `climate-safeguards-terraform` bucket). Their states were merged into `quotaclimat-terraform-states/misinformation/<env>/terraform.tfstate` by `bin/migrate_climateguard_state.sh` (dry run by default, `--push` to write), without changing any resource: the resulting plan is empty. Dev's `france` resources, migrated with the rest, were then removed from the `countries` map and destroyed. The climateguard states are left in place as a backup; do not apply from climateguard any more. The `climate-safeguards-<env>-tofu` IAM applications that climateguard deployed with are no longer used (this target uses the shared key of `.env`) and can be deleted.

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

### `misinformation` (`live/misinformation/template/`)
Resources marked *per country* are `for_each` over the `countries` input (address `<resource>["<country>"]`).

| Resource | Address | Purpose |
|---|---|---|
| Scaleway Project | `scaleway_account_project.project` | `climate-safeguards-<env>` project |
| Container Namespace | `scaleway_container_namespace.container_namespace` | Namespace for the Label Studio containers |
| Container | `scaleway_container.labelstudio_container` | Label Studio serverless container, per country |
| RDB user | `scaleway_rdb_user.labelstudio_user` | Label Studio DB user on the barometre instance, per country |
| RDB database | `scaleway_rdb_database.labelstudio_db` | Label Studio database, per country |
| RDB privilege | `scaleway_rdb_privilege.labelstudio_policy` | Grants for `labelstudio_user`, per country |
| IAM Application | `scaleway_iam_application.project_application` | App identity of the container and the job, per country |
| IAM Policy | `scaleway_iam_policy.project_application` | Permissions for `project_application` (this project and barometre), per country |
| IAM API key | `scaleway_iam_api_key.project_api_key` | Credentials for `project_application`, per country |
| Object Storage bucket | `scaleway_object_bucket.bucket` | `safeguards-climate-<country>-<env>` output bucket, per country |
| Bucket policy | `scaleway_object_bucket_policy.bucket_policy` | Access for `project_application` (and one user, TEMP), per country |
| Secret | `scaleway_secret.{mediatree_password,openai_api_key,mistral_api_key,postgres_password_barometre}` (+ `_version`) | Shared job secrets, path `/common` |
| Secret | `scaleway_secret.{access_key_id,secret_key_id,secret_label_studio_token}` (+ `_version`) | Bucket keys and Label Studio token, path `/<country>`, per country |
| Serverless job | `scaleway_job_definition.main` | `label-misinformation-<country>-<env>`, per country |

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
| RDB user | `scaleway_rdb_user.metabase_analytics` | Readonly user (`metabase-analytics-<env>`) for Metabase dashboards |
| RDB privilege | `scaleway_rdb_privilege.metabase_analytics` | Readonly grant for `metabase_analytics` on `barometre` |
| Terraform data (null resource) | `terraform_data.readonly_default_privileges` | Default privileges of the database admin (`barometreclimat` in prod): the tables and schemas it creates next (Alembic migrations), in any schema, are readable by `metabase_analytics` and `rrs_read`, which Scaleway's `readonly` privilege doesn't cover (it only sets defaults for `_rdb_superadmin`); plus the catch-up grant on `advertising.ad_screenshot` |
| RDB ACL | `scaleway_rdb_acl.public` | IP allow-list (Scaleway job CIDRs + dev/prod extra IPs) |
| Instance IP | `scaleway_instance_ip.gpu` | Flexible IP for the GPU instance |
| Instance server | `scaleway_instance_server.gpu` | GPU instance (Whisper transcription / ML workloads) |
| Object bucket | `scaleway_object_bucket.mediatree_videos` | Stores Mediatree video files |
| IAM Application | `scaleway_iam_application.mediatree_videos_application` | App identity scoped to the `mediatree-videos` bucket (project-wide in practice) |
| IAM Policy | `scaleway_iam_policy.mediatree_videos_policy` | Object storage read/write for `mediatree_videos_application` |
| IAM API key | `scaleway_iam_api_key.mediatree_videos_api_key` | Credentials for `mediatree_videos_application` |
| IAM Application | `scaleway_iam_application.registry_cleanup` | Identity of the registry cleanup job (only when `registry_cleanup_enabled`, prod) |
| IAM Policy | `scaleway_iam_policy.registry_cleanup` | Registry full access on the project, jobs/containers read-only on the organization |
| IAM API key | `scaleway_iam_api_key.registry_cleanup` | Credentials for `registry_cleanup`, stored in the secret below |
| Secret | `scaleway_secret.registry_cleanup_secret_key` (+ `_version`) | Secret key of the cleanup job, path `/registry-cleanup` |
| Job definition | `scaleway_job_definition.registry_cleanup` | `registry-cleanup-<env>`, weekly (Sunday 03:17 Europe/Paris): deletes the tags of `registry_namespace_name` (`namespace-barometre` in prod) except the `registry_cleanup_keep` (10) most recent per image, `latest` and the tags used by a job or container of the organization (`scripts/registry_cleanup.py`, inline in `python:3.12-alpine`). `registry_cleanup_dry_run = true` only logs what it would delete |

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
| RDB user | `scaleway_rdb_user.windmill` | Windmill app DB user, Scaleway admin only while `TF_VAR_windmill_bootstrap=true` |
| Terraform data | `terraform_data.windmill_database` | Creates the `windmill` database (owned by the `windmill` user) and the `windmill_admin`/`windmill_user` roles with `psql`; drops them on destroy |
| IAM SSH key | `scaleway_iam_ssh_key.paul_gabriel` | SSH access to the baremetal server |
| IAM SSH key | `scaleway_iam_ssh_key.gmguarino` | SSH access to the baremetal server |
| Object bucket | `scaleway_object_bucket.glitchtip` | GlitchTip file storage and cold storage (`cold_storage/` prefix) |
| IAM application | `scaleway_iam_application.glitchtip_s3` | Identity used only by GlitchTip |
| IAM policy | `scaleway_iam_policy.glitchtip_s3` | Object Storage read/write on the orchestrator project only (Scaleway combines it with the bucket policy) |
| IAM API key | `scaleway_iam_api_key.glitchtip_s3` | Access/secret key, read with `make env=prod target=orchestrator tg-output name=glitchtip_s3_secret_key` |
| Bucket policy | `scaleway_object_bucket_policy.glitchtip` | Read/write on the GlitchTip bucket only; needs `project_id` or Scaleway answers 403 |
| Baremetal server | `scaleway_baremetal_server.orchestrator` | Elastic Metal server (EM-A610R-NVMe) hosting Kestra + GlitchTip via Docker Compose/Ansible |
