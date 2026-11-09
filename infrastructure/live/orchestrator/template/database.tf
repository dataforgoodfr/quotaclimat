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
# Windmill's migrations create its own roles (windmill_admin, windmill_user), so the
# owner user needs CREATEROLE-like rights on the managed instance.

resource "scaleway_rdb_user" "windmill" {
  instance_id = data.scaleway_rdb_instance.shared.id
  name        = "windmill"
  password    = var.postgres_password_windmill
}

resource "scaleway_rdb_database" "windmill" {
  instance_id = data.scaleway_rdb_instance.shared.id
  name        = "windmill"
}

resource "scaleway_rdb_privilege" "windmill" {
  instance_id   = data.scaleway_rdb_instance.shared.id
  user_name     = scaleway_rdb_user.windmill.name
  database_name = scaleway_rdb_database.windmill.name
  permission    = "all"
  depends_on    = [scaleway_rdb_user.windmill, scaleway_rdb_database.windmill]
}
