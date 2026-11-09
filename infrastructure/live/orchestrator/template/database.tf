# Databases for Kestra, GlitchTip and Windmill on the existing managed Postgres instance.

data "scaleway_account_project" "pg_project" {
  name = var.pg_project_name
}

data "scaleway_rdb_instance" "shared" {
  name       = var.pg_instance_name
  project_id = data.scaleway_account_project.pg_project.id
}

# --- Kestra ---

resource "scaleway_rdb_user" "kestra" {
  instance_id = data.scaleway_rdb_instance.shared.id
  name        = "kestra"
  password    = var.postgres_password_kestra
}

resource "scaleway_rdb_database" "kestra" {
  instance_id = data.scaleway_rdb_instance.shared.id
  name        = "kestra"
}

resource "scaleway_rdb_privilege" "kestra" {
  instance_id   = data.scaleway_rdb_instance.shared.id
  user_name     = scaleway_rdb_user.kestra.name
  database_name = scaleway_rdb_database.kestra.name
  permission    = "all"
  depends_on    = [scaleway_rdb_user.kestra, scaleway_rdb_database.kestra]
}

# --- GlitchTip ---

resource "scaleway_rdb_user" "glitchtip" {
  instance_id = data.scaleway_rdb_instance.shared.id
  name        = "glitchtip"
  password    = var.postgres_password_glitchtip
}

resource "scaleway_rdb_database" "glitchtip" {
  instance_id = data.scaleway_rdb_instance.shared.id
  name        = "glitchtip"
}

resource "scaleway_rdb_privilege" "glitchtip" {
  instance_id   = data.scaleway_rdb_instance.shared.id
  user_name     = scaleway_rdb_user.glitchtip.name
  database_name = scaleway_rdb_database.glitchtip.name
  permission    = "all"
  depends_on    = [scaleway_rdb_user.glitchtip, scaleway_rdb_database.glitchtip]
}

# --- Windmill ---
# Only the user: the windmill database is created and owned by this user itself (grants.tf), so that it owns the
# `public` schema and can grant it to the Windmill roles. is_admin (CREATEDB + CREATEROLE) is only needed while
# bootstrapping the database and roles, and before a destroy: set TF_VAR_windmill_bootstrap=true for those applies.

resource "scaleway_rdb_user" "windmill" {
  instance_id = data.scaleway_rdb_instance.shared.id
  name        = "windmill"
  password    = var.postgres_password_windmill
  is_admin    = var.windmill_bootstrap
}
