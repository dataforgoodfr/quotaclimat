locals {
  # Dev's instance root admin doubles as the database admin (see database_admin_password).
  database_admin_password = var.database_admin_user == var.postgres_admin_user ? var.postgres_admin_password : var.database_admin_password
  readonly_users          = [scaleway_rdb_user.metabase_analytics.name, scaleway_rdb_user.rrs_read.name]
  readonly_users_sql      = join(", ", [for user in local.readonly_users : "\\\"${user}\\\""])
}

# The readonly privilege of Scaleway grants select on the tables that exist when it is set, plus default
# privileges for the tables created by _rdb_superadmin only. The tables created afterwards by the Alembic
# migrations, as the database admin (barometreclimat in prod), were not readable by the readonly users
# (e.g. advertising.ad_screenshot for Metabase).
# Default privileges of the database admin, without IN SCHEMA: every table and schema it creates next, in any
# schema, is readable by the readonly users. Then the catch-up of the tables created before.
resource "terraform_data" "readonly_default_privileges" {
  triggers_replace = [
    local.readonly_users,
    scaleway_rdb_database.barometre.name,
  ]

  provisioner "local-exec" {
    command = <<-EOF
      docker run --rm \
        -e PGPASSWORD="${local.database_admin_password}" \
        postgres:15 psql -v ON_ERROR_STOP=1 \
          "host=${scaleway_rdb_instance.barometre_rdb.endpoint_ip} port=${scaleway_rdb_instance.barometre_rdb.endpoint_port} dbname=${scaleway_rdb_database.barometre.name} user=${var.database_admin_user} sslmode=require" \
          -c "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO ${local.readonly_users_sql};" \
          -c "ALTER DEFAULT PRIVILEGES GRANT USAGE ON SCHEMAS TO ${local.readonly_users_sql};" \
          -c "GRANT SELECT ON advertising.ad_screenshot TO ${local.readonly_users_sql};"
    EOF
  }

  depends_on = [
    scaleway_rdb_privilege.barometre_admin,
    scaleway_rdb_privilege.metabase_analytics,
    scaleway_rdb_privilege.rrs_read,
  ]
}
