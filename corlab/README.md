# corlab

Standalone deployment of **PostgreSQL + Metabase** on a separate, already provisioned Ubuntu server, with Ansible. It follows `infrastructure/ansible` (same `common` role, copied so that corlab stays independent) and applies the repo's `alembic/` migrations to a `barometre` database.

No Terragrunt/OpenTofu: the server already exists. There is **no reverse proxy** here: Metabase is published on its standard port, and a proxy on a separate machine can be added later (see below).

## What runs where

| Service | Dir on server | Notes |
|---|---|---|
| PostgreSQL 15 | `/opt/services/postgres` | Docker network `corlab-internal` (Metabase connects to `postgres:5432`); published on all interfaces on the standard port `5432`, for external services. Databases: `barometre` (data) and `metabase` (Metabase application DB, own role) |
| Metabase | `/opt/services/metabase` | Published on all interfaces on the standard port `3000` (plain HTTP); application DB in `metabase` |
| Migrations | `/opt/services/migrations` | One-shot image built on the server, runs `alembic upgrade head` on `barometre` |

Open ports (UFW, and the upstream firewall must match): `CORLAB_SSH_PORT` (2251, admin), 5432 (Postgres, external services) and 3000 (Metabase).

```
INTERNET ── 3000 ─▶ Metabase ──(corlab-internal)──▶ PostgreSQL :5432
         ── 5432 ───────────────────────────────────▶ PostgreSQL :5432
         ── 2251 ─▶ sshd (Ansible, make psql)
```

**Exposure**: Postgres and Metabase are published on all interfaces, with no IP filtering in this repo (Docker publishes ports around UFW, so UFW would not filter them anyway). Access control is the job of the network firewall in front of the server, which also handles the redirection from the separate reverse proxy machine. Use a strong `CORLAB_PG_PASSWORD` and enable `ssl=on` with `hostssl` rules in `pg_hba.conf` (not configured yet): the protocol is plaintext by default, and Metabase is plain HTTP until a proxy terminates TLS.

## First-time deployment

1. Make sure the ports above are open between the clients and the server (upstream firewall).
2. `cd corlab && make setup` (installs Ansible with mise, plus the `community.docker`, `community.general` and `ansible.posix` collections).
3. Create `corlab/.env` (git-ignored): `cp .envdist .env` and fill it in. `.envdist` lists every variable (summary below):
   ```bash
   # Server
   CORLAB_HOST_IP=
   CORLAB_SSH_PORT=2251
CORLAB_SSH_USER=ubuntu           # account used to log in
CORLAB_DEPLOY_USER=              # optional: account to create/use for the deployment (default: CORLAB_SSH_USER)
CORLAB_DEPLOY_USER_PASSWORD=     # optional: its password, only set when the account is created
CORLAB_SSH_PASSWORD=             # SSH password (needs sshpass locally)
CORLAB_BECOME_PASSWORD=          # sudo password, optional (defaults to the SSH password)
   # Postgres (instance superuser, owner of barometre)
   CORLAB_PG_USER=corlab
   CORLAB_PG_PASSWORD=
   CORLAB_BAROMETRE_DB=barometre    # optional
   # Read-only roles (optional passwords, see "Database roles")
   CORLAB_RRS_READ_DEV_PASSWORD=
   CORLAB_RRS_READ_PROD_PASSWORD=
   CORLAB_CLIMATEGUARD_READER_PASSWORD=
   CORLAB_METABASE_ANALYTICS_PROD_PASSWORD=
   # Metabase application database
   CORLAB_METABASE_DB_PASSWORD=
   ```
   Password generator: `LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32`.
4. `make syntax`, then `make check` (dry run), then `make ansible`.
5. `make migrate` to apply the migrations.
6. Open Metabase, create the admin account, add the `barometre` database (host `postgres`, port `5432`, the `CORLAB_PG_*` credentials, or a read-only role).

## Tags

`make tags=<tag> ansible` with: `user`, `common`, `postgres`, `metabase`. The `migrations` role is tagged `never`: it only runs through `make migrate` (`--tags migrate`). Re-run it after each new migration in `alembic/versions/`.

## Migrations

The role archives `pyproject.toml`, `poetry.lock`, `alembic.ini`, `alembic/`, `postgres/` and `quotaclimat/` from this repo (the machine running Ansible), unpacks the archive on the server, builds `corlab/migrations/Dockerfile` there (full Poetry install: several migrations import `quotaclimat` modules) and runs `alembic upgrade head` in a container on `corlab-internal`. The checked-out repo state is what gets applied: check out the commit you want first. Migration `5833799c8abc` creates `pgcrypto`, which needs the superuser `CORLAB_PG_USER`.

## Database roles

The alembic migrations create no role. The `postgres` role creates, before the migrations, the read-only roles that dbt grants `select` to when `DBT_ENV=prod` (`+grants` in `my_dbt_project/dbt_project.yml`, regular perimeter): `rrs-read-dev`, `rrs-read-prod`, `climateguard-reader-user` and `metabase-analytics-prod`. The `metabase-analytics-dev` role (dev only) is not created. Each gets `CONNECT` on `barometre`, `USAGE` on every schema and `SELECT` on every table, plus default privileges (as `CORLAB_PG_USER`, which runs the migrations) so the tables and schemas created afterwards are readable too, as in `infrastructure/live/barometre/template/grants.tf`. A role has `LOGIN` and a password only if its `CORLAB_*_PASSWORD` is set, otherwise it is `NOLOGIN` (enough for dbt grants to apply). Re-run `make tags=postgres ansible` to catch up on tables created by other users. The `grant_schema_usage` dbt hook adds `USAGE` on the schemas dbt builds. For the extended perimeter, which only uses `rrs-read-prod` in prod, the other roles are simply unused.

## Access to Postgres

- `make psql`: psql on `barometre` over SSH.
- External services: `<server>:5432`, database `barometre`.
- `make tunnel` (then `psql -h localhost -p 5433 -U <user> barometre`): SSH port forward, for when 5432 is closed to your IP.

## Notes

- **Deploy user**: the `user` role creates `CORLAB_DEPLOY_USER` (default: `CORLAB_SSH_USER`) with a bash shell, home and the `sudo` group if it doesn't exist; the `common` role adds it to `docker`. To bootstrap a new account, log in as an existing admin (`CORLAB_SSH_USER=root` or `ubuntu`) with `CORLAB_DEPLOY_USER=<new>` and `CORLAB_DEPLOY_USER_PASSWORD`, then switch `CORLAB_SSH_USER` to the new account. A password is hashed on your machine, which needs `passlib` (`pip install passlib`). An existing user's password is never changed.
- **SSH port**: `CORLAB_SSH_PORT` (default 2251) is the port Ansible, `make tunnel` and `make psql` connect to. It can differ from the port sshd listens on (port forwarding/NAT), so the `common` role reads sshd's real ports (`sshd -T`, read-only) and opens both in UFW and fail2ban before enabling the firewall. It never changes sshd's own `Port`.
- **Password SSH**: Ansible connects with `CORLAB_SSH_PASSWORD` (public-key auth is disabled in the inventory), which requires `sshpass` on your machine. sshd is not configured by corlab: no `sshd_config` change, no `authorized_keys` change, no sshd restart. `make tunnel`/`make psql` use plain `ssh` and prompt for the password.
- SSH keys are not managed: Ansible does not touch `authorized_keys`, access stays as it is on the server.
- `metabase_version` defaults to `latest` (`ansible/roles/metabase/defaults/main.yml`); pin a version before relying on it.
- **Reverse proxy later**: it will run on a separate machine. Point it at `http://<this-server>:3000`; the network firewall handles the redirection.
- No backups are configured. The Postgres data lives in the Docker volume `postgres_postgres-data`.
