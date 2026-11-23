# corlab

Standalone deployment of **Traefik + PostgreSQL + Metabase** on a separate, already provisioned Ubuntu server, with Ansible. It follows `infrastructure/ansible` (same `common`, `ssh-keys` and `traefik` roles, copied so that corlab stays independent) and applies the repo's `alembic/` migrations to a `barometre` database.

No Terragrunt/OpenTofu: the server and its DNS records already exist.

## What runs where

| Service | Dir on server | Notes |
|---|---|---|
| Traefik v3 | `/opt/services/traefik` | Let's Encrypt (HTTP-01), dashboard behind basic auth |
| PostgreSQL 15 | `/opt/services/postgres` | Docker network `corlab-internal` (Metabase connects to `postgres:5432`); published on all interfaces as `6436` (container 5432) for external services. Databases: `barometre` (data) and `metabase` (Metabase application DB, own role) |
| Metabase | `/opt/services/metabase` | `https://<CORLAB_METABASE_DOMAIN>` through Traefik, no published port (container 3000), application DB in `metabase` |
| Migrations | `/opt/services/migrations` | One-shot image built on the server, runs `alembic upgrade head` on `barometre` |

Open ports (UFW, and the upstream firewall must match): `CORLAB_SSH_PORT` (2251, admin), 80 (HTTP to HTTPS redirect and Let's Encrypt HTTP-01), 443 (Traefik, Metabase) and 6436 (Postgres, for external services).

```
INTERNET ── 80 ─▶ Traefik (301 to https, ACME HTTP-01)
         ── 443 ─▶ Traefik ─▶ Metabase :3000 ──(corlab-internal)──▶ PostgreSQL :5432
         ── 6436 ───────────────────────────────────────────────────▶ PostgreSQL :5432
         ── 2251 ─▶ sshd (Ansible, make psql)
```

**Postgres exposure**: Docker publishes `6436` around UFW, so the `postgres` role restricts it in the `DOCKER-USER` iptables chain: only the IPs in `CORLAB_PG_ALLOWED_IPS` (comma-separated IPv4/CIDR, at least one, validated by the playbook) can connect, everything else to `6436` is dropped. The rules live in a `CORLAB-PG` chain applied by `/usr/local/sbin/corlab-docker-user.sh` and the `corlab-docker-user` systemd unit (re-applied after each Docker start). Container-to-container traffic and SSH tunnels are not affected. Also use a strong `CORLAB_PG_PASSWORD` and enable `ssl=on` with `hostssl` rules in `pg_hba.conf` (not configured yet): the protocol is plaintext by default. Check with `sudo iptables -L DOCKER-USER -n -v` and `sudo iptables -L CORLAB-PG -n -v`.

## First-time deployment

1. DNS: A records for `CORLAB_TRAEFIK_DOMAIN` and `CORLAB_METABASE_DOMAIN` pointing to the server (needed for the Let's Encrypt challenge).
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
   # Traefik / TLS
   ACME_EMAIL=
   CORLAB_TRAEFIK_DOMAIN=traefik.corlab.example.org
   CORLAB_METABASE_DOMAIN=metabase.corlab.example.org
   CORLAB_TRAEFIK_DASHBOARD_AUTH=   # echo $(htpasswd -nB admin) | sed -e s/\\$/\\$\\$/g
   # Postgres (instance superuser, owner of barometre)
   CORLAB_PG_USER=corlab
   CORLAB_PG_PASSWORD=
   CORLAB_PG_ALLOWED_IPS=           # comma-separated IPv4/CIDR allowed on 6436, e.g. 203.0.113.10,198.51.100.7 (at least one)
   CORLAB_BAROMETRE_DB=barometre    # optional
   # Metabase application database
   CORLAB_METABASE_DB_PASSWORD=
   ```
   Password generator: `LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32`.
4. `make syntax`, then `make check` (dry run), then `make ansible`.
5. `make migrate` to apply the migrations.
6. Open Metabase, create the admin account, add the `barometre` database (host `postgres`, port `5432`, the `CORLAB_PG_*` credentials, or a read-only role).

## Tags

`make tags=<tag> ansible` with: `user`, `ssh-keys`, `common`, `traefik`, `postgres`, `metabase`. The `migrations` role is tagged `never`: it only runs through `make migrate` (`--tags migrate`). Re-run it after each new migration in `alembic/versions/`.

## Migrations

The role syncs `pyproject.toml`, `poetry.lock`, `alembic.ini`, `alembic/`, `postgres/` and `quotaclimat/` from this repo (the machine running Ansible) to the server, builds `corlab/migrations/Dockerfile` there (full Poetry install: several migrations import `quotaclimat` modules) and runs `alembic upgrade head` in a container on `corlab-internal`. The checked-out repo state is what gets applied: check out the commit you want first. Migration `5833799c8abc` creates `pgcrypto`, which needs the superuser `CORLAB_PG_USER`.

## Access to Postgres

- `make psql`: psql on `barometre` over SSH.
- External services: `<server>:6436`, database `barometre`.
- `make tunnel` (then `psql -h localhost -p 5433 -U <user> barometre`): SSH port forward, for when 6436 is closed to your IP.

## Notes

- **Deploy user**: the `user` role creates `CORLAB_DEPLOY_USER` (default: `CORLAB_SSH_USER`) with a bash shell, home and the `sudo` group if it doesn't exist; the `common` role adds it to `docker`. To bootstrap a new account, log in as an existing admin (`CORLAB_SSH_USER=root` or `ubuntu`) with `CORLAB_DEPLOY_USER=<new>` and `CORLAB_DEPLOY_USER_PASSWORD`, then switch `CORLAB_SSH_USER` to the new account. A password is hashed on your machine, which needs `passlib` (`pip install passlib`). An existing user's password is never changed.
- **SSH port**: sshd listens on `CORLAB_SSH_PORT` (default 2251). Ansible, `make tunnel` and `make psql` use it, and the `common` role opens it in UFW and fail2ban instead of 22. The role does not change sshd's own `Port`.
- **Password SSH**: Ansible connects with `CORLAB_SSH_PASSWORD` (public-key auth is disabled in the inventory), which requires `sshpass` on your machine. The `common` role leaves sshd's `PasswordAuthentication` untouched (`ssh_disable_password_auth: false` in `roles/common/defaults/main.yml`) and sets `MaxAuthTries` to 6, so it cannot lock you out. Once key access works, switch to keys: remove the password lines from `ansible/inventory.yml` and set that variable to `true`. `make tunnel`/`make psql` use plain `ssh` and prompt for the password.
- The `ssh-keys` role copied from `infrastructure` authorizes the same team keys (root and the deploy user) on this server: edit `ansible/roles/ssh-keys/tasks/main.yml` if the access list should differ.
- `metabase_version` defaults to `latest` (`ansible/roles/metabase/defaults/main.yml`); pin a version before relying on it.
- No backups are configured. The Postgres data lives in the Docker volume `postgres_postgres-data`.
