# Windmill database bootstrap, run as the Windmill user itself.
#
# The database is created by the `windmill` user with `CREATE DATABASE`, not by Scaleway's API
# (scaleway_rdb_database): a database created by the API is owned by `_rdb_superadmin`, and so is its `public`
# schema, on which Scaleway strips the default USAGE grant. Nobody but `_rdb_superadmin` could then grant USAGE on
# `public` to the Windmill roles, and every query would fail with "permission denied for schema public". As the
# owner of its database, `windmill` owns `public` and grants it itself
# (https://www.windmill.dev/docs/advanced/self_host#use-an-external-database).
#
# Creating a database and roles needs CREATEDB and CREATEROLE: `windmill` is a Scaleway admin user only while
# var.windmill_bootstrap is true (see database.tf and the Readme for the two-apply flow).
#
# `windmill_admin` is created WITHOUT BYPASSRLS: the managed instance has no superuser, and only a superuser can
# create a BYPASSRLS role. Recent Windmill versions apply the equivalent row-level-security policies to
# `windmill_admin` in a migration (https://github.com/windmill-labs/windmill/issues/2345), so the image must stay
# recent (see windmill_image). If the migrations still fail with "permission denied for table", set
# MIGRATION_NO_BYPASSRLS=true on the server and workers.
#
# Roles are cluster-wide on Postgres: they are shared by every database of the instance, but only `windmill` is
# a member of them.
#
# The connection and password go through `environment`, not the command: OpenTofu prints the command when a
# provisioner fails. Destroy provisioners can only read `self`, hence `input`.
resource "terraform_data" "windmill_database" {
  # Replacing this resource drops the database (destroy provisioner): only a new user does that.
  triggers_replace = [scaleway_rdb_user.windmill.name]

  input = {
    conn     = "host=${data.scaleway_rdb_instance.shared.endpoint_ip} port=${data.scaleway_rdb_instance.shared.endpoint_port} user=${scaleway_rdb_user.windmill.name} sslmode=require"
    password = var.postgres_password_windmill
  }

  # CREATE DATABASE has no IF NOT EXISTS and cannot run in a transaction or a DO block: psql generates it only when
  # the database is missing (\gexec), from Scaleway's default database `rdb`.
  provisioner "local-exec" {
    environment = {
      CONN   = self.input.conn
      PGPASS = self.input.password
    }
    command = <<-EOF
      set -eu
      docker run --rm -i -e PGPASSWORD="$PGPASS" postgres:15 \
        psql -v ON_ERROR_STOP=1 "$CONN dbname=rdb" -f - <<'SQL'
      SELECT 'CREATE DATABASE windmill' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'windmill')\gexec
      SQL
      docker run --rm -i -e PGPASSWORD="$PGPASS" postgres:15 \
        psql -v ON_ERROR_STOP=1 "$CONN dbname=windmill" -f - <<'SQL'
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
      GRANT windmill_admin TO CURRENT_USER;
      GRANT windmill_user TO CURRENT_USER;

      -- Windmill switches to windmill_user / windmill_admin to run queries
      GRANT USAGE ON SCHEMA public TO windmill_user, windmill_admin;

      -- Tables and sequences created by the migrations (as the current user) stay readable by the Windmill roles
      GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO windmill_user;
      GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO windmill_user;
      ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO windmill_user;
      ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO windmill_user;
      SQL
    EOF
  }

  # Dropping the roles needs CREATEROLE on Postgres 15 and older: apply with TF_VAR_windmill_bootstrap=true before
  # removing Windmill. WITH (FORCE) (Postgres 13+) closes the connections Windmill still has.
  provisioner "local-exec" {
    when = destroy
    environment = {
      CONN   = self.input.conn
      PGPASS = self.input.password
    }
    command = <<-EOF
      docker run --rm -e PGPASSWORD="$PGPASS" postgres:15 \
        psql -v ON_ERROR_STOP=1 "$CONN dbname=rdb" \
          -c "DROP DATABASE IF EXISTS windmill WITH (FORCE);" \
          -c "DROP ROLE IF EXISTS windmill_admin;" \
          -c "DROP ROLE IF EXISTS windmill_user;"
    EOF
  }
}
