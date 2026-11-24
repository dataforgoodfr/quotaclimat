# corlab

Standalone deployment of **PostgreSQL + Metabase** on a separate, already provisioned Ubuntu server, with Ansible. It follows `infrastructure/ansible` (same `common` and `ssh-keys` roles, copied so that corlab stays independent) and applies the repo's `alembic/` migrations to a `barometre` database.

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

**Source-IP restriction**: Docker publishes ports around UFW, so the `postgres` role restricts both ports in the `DOCKER-USER` iptables chain. Only the IPs in `CORLAB_PG_ALLOWED_IPS` (required, comma-separated IPv4/CIDR) can reach `5432`, and only those in `CORLAB_METABASE_ALLOWED_IPS` (optional) can reach `3000`; with an empty Metabase list, `3000` is not filtered. Everything else is dropped. The rules live in `CORLAB-PG` and `CORLAB-MB` chains applied by `/usr/local/sbin/corlab-docker-user.sh` and the `corlab-docker-user` systemd unit (re-applied after each Docker start). Container-to-container traffic and SSH tunnels are not affected. Also use a strong `CORLAB_PG_PASSWORD` and enable `ssl=on` with `hostssl` rules in `pg_hba.conf` (not configured yet): the protocol is plaintext by default, and Metabase is plain HTTP until a proxy terminates TLS. Check with `sudo iptables -L DOCKER-USER -n -v`, `sudo iptables -L CORLAB-PG -n -v` and `sudo iptables -L CORLAB-MB -n -v`.

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
   CORLAB_PG_ALLOWED_IPS=           # comma-separated IPv4/CIDR allowed on 5432, e.g. 203.0.113.10,198.51.100.7 (at least one)
   CORLAB_BAROMETRE_DB=barometre    # optional
   # Metabase application database, and optional IPs allowed on 3000
   CORLAB_METABASE_DB_PASSWORD=
   CORLAB_METABASE_ALLOWED_IPS=     # optional, e.g. the reverse proxy machine
   ```
   Password generator: `LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32`.
4. `make syntax`, then `make check` (dry run), then `make ansible`.
5. `make migrate` to apply the migrations.
6. Open Metabase, create the admin account, add the `barometre` database (host `postgres`, port `5432`, the `CORLAB_PG_*` credentials, or a read-only role).

## Tags

`make tags=<tag> ansible` with: `user`, `ssh-keys`, `common`, `postgres`, `metabase`. The `migrations` role is tagged `never`: it only runs through `make migrate` (`--tags migrate`). Re-run it after each new migration in `alembic/versions/`.

## Migrations

The role syncs `pyproject.toml`, `poetry.lock`, `alembic.ini`, `alembic/`, `postgres/` and `quotaclimat/` from this repo (the machine running Ansible) to the server, builds `corlab/migrations/Dockerfile` there (full Poetry install: several migrations import `quotaclimat` modules) and runs `alembic upgrade head` in a container on `corlab-internal`. The checked-out repo state is what gets applied: check out the commit you want first. Migration `5833799c8abc` creates `pgcrypto`, which needs the superuser `CORLAB_PG_USER`.

## Access to Postgres

- `make psql`: psql on `barometre` over SSH.
- External services: `<server>:5432`, database `barometre`.
- `make tunnel` (then `psql -h localhost -p 5433 -U <user> barometre`): SSH port forward, for when 5432 is closed to your IP.

## Notes

- **Deploy user**: the `user` role creates `CORLAB_DEPLOY_USER` (default: `CORLAB_SSH_USER`) with a bash shell, home and the `sudo` group if it doesn't exist; the `common` role adds it to `docker`. To bootstrap a new account, log in as an existing admin (`CORLAB_SSH_USER=root` or `ubuntu`) with `CORLAB_DEPLOY_USER=<new>` and `CORLAB_DEPLOY_USER_PASSWORD`, then switch `CORLAB_SSH_USER` to the new account. A password is hashed on your machine, which needs `passlib` (`pip install passlib`). An existing user's password is never changed.
- **SSH port**: sshd listens on `CORLAB_SSH_PORT` (default 2251). Ansible, `make tunnel` and `make psql` use it, and the `common` role opens it in UFW and fail2ban instead of 22. The role does not change sshd's own `Port`.
- **Password SSH**: Ansible connects with `CORLAB_SSH_PASSWORD` (public-key auth is disabled in the inventory), which requires `sshpass` on your machine. The `common` role leaves sshd's `PasswordAuthentication` untouched (`ssh_disable_password_auth: false` in `roles/common/defaults/main.yml`) and sets `MaxAuthTries` to 6, so it cannot lock you out. Once key access works, switch to keys: remove the password lines from `ansible/inventory.yml` and set that variable to `true`. `make tunnel`/`make psql` use plain `ssh` and prompt for the password.
- The `ssh-keys` role copied from `infrastructure` authorizes the same team keys (root and the deploy user) on this server: edit `ansible/roles/ssh-keys/tasks/main.yml` if the access list should differ.
- `metabase_version` defaults to `latest` (`ansible/roles/metabase/defaults/main.yml`); pin a version before relying on it.
- **Reverse proxy later**: it will run on a separate machine. Point it at `http://<this-server>:3000` and add its IP to `CORLAB_METABASE_ALLOWED_IPS`, then run `make tags=postgres ansible` to apply the filter.
- No backups are configured. The Postgres data lives in the Docker volume `postgres_postgres-data`.
