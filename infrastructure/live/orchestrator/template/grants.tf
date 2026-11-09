locals {
  windmill_db_user = scaleway_rdb_user.windmill.name
  windmill_db_name = scaleway_rdb_database.windmill.name
}

# A new database is only accessible to the users granted on it: the instance admin that creates the roles
# below needs its own privilege on the windmill database, or psql fails with "User does not have CONNECT
# privilege".
resource "scaleway_rdb_privilege" "windmill_admin" {
  instance_id   = data.scaleway_rdb_instance.shared.id
  user_name     = var.postgres_admin_user
  database_name = scaleway_rdb_database.windmill.name
  permission    = "all"
}

# Windmill's migrations expect two roles to exist, `windmill_user` and `windmill_admin`, and need the
# application user to be a member of `windmill_admin`. This is Windmill's init script for an external Postgres
# (https://www.windmill.dev/docs/advanced/self_host#use-an-external-database), made idempotent.
# `windmill_admin` is created WITHOUT BYPASSRLS: the managed instance has no superuser, and only a superuser can
# create a BYPASSRLS role. Recent Windmill versions apply the equivalent row-level-security policies to
# `windmill_admin` in a migration (https://github.com/windmill-labs/windmill/issues/2345), so the image must stay
# recent (see windmill_image). If the migrations still fail with "permission denied for table", set
# MIGRATION_NO_BYPASSRLS=true on the server and workers.
# Roles are cluster-wide on Postgres: they are shared by every database of the instance, but only this
# application user is granted `windmill_admin`.
resource "terraform_data" "windmill_roles" {
  triggers_replace = [
    local.windmill_db_user,
    local.windmill_db_name,
  ]

  provisioner "local-exec" {
    command = <<-EOF
      docker run --rm -i \
        -e PGPASSWORD="${var.postgres_admin_password}" \
        postgres:15 psql -v ON_ERROR_STOP=1 \
          "host=${data.scaleway_rdb_instance.shared.endpoint_ip} port=${data.scaleway_rdb_instance.shared.endpoint_port} dbname=${local.windmill_db_name} user=${var.postgres_admin_user} sslmode=require" \
          -f - <<'SQL'
      DO $$
      BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'windmill_user') THEN
          CREATE ROLE windmill_user;
        END IF;
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'windmill_admin') THEN
          CREATE ROLE windmill_admin;
        END IF;
      END
      $$;

      GRANT windmill_user TO windmill_admin;
      GRANT windmill_admin TO "${local.windmill_db_user}";

      -- The public schema belongs to the database owner (_rdb_superadmin), the only role that can grant on it.
      -- Windmill switches to windmill_user / windmill_admin to run queries, which need USAGE on it.
      -- The application user already has USAGE and CREATE on public (granted by Scaleway).
      SET ROLE _rdb_superadmin;
      GRANT USAGE ON SCHEMA public TO windmill_admin, windmill_user;
      RESET ROLE;

      GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO windmill_user;
      GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO windmill_user;
      ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO windmill_user;
      ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO windmill_user;
      SQL
    EOF
  }

  depends_on = [
    scaleway_rdb_privilege.windmill,
    scaleway_rdb_privilege.windmill_admin,
  ]
}
