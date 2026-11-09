locals {
  windmill_db_user = scaleway_rdb_user.windmill.name
  windmill_db_name = scaleway_rdb_database.windmill.name
}

# Windmill's migrations expect two roles to exist, `windmill_user` and `windmill_admin` (BYPASSRLS), and need
# the application user to be a member of `windmill_admin`. This is Windmill's own init script for an external
# Postgres (https://www.windmill.dev/docs/advanced/self_host#use-an-external-database), made idempotent. It is
# run by the instance admin because the application user cannot create a BYPASSRLS role on the managed instance.
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
          CREATE ROLE windmill_admin WITH BYPASSRLS;
        END IF;
      END
      $$;

      GRANT windmill_user TO windmill_admin;
      GRANT windmill_admin TO "${local.windmill_db_user}";

      -- Postgres 15+ no longer lets non-owners create objects in public by default
      GRANT ALL ON SCHEMA public TO "${local.windmill_db_user}", windmill_admin, windmill_user;

      GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO windmill_user;
      GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO windmill_user;
      ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO windmill_user;
      ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO windmill_user;
      ALTER DEFAULT PRIVILEGES FOR ROLE "${local.windmill_db_user}" IN SCHEMA public GRANT ALL ON TABLES TO windmill_user;
      ALTER DEFAULT PRIVILEGES FOR ROLE "${local.windmill_db_user}" IN SCHEMA public GRANT ALL ON SEQUENCES TO windmill_user;
      SQL
    EOF
  }

  depends_on = [
    scaleway_rdb_privilege.windmill,
  ]
}
