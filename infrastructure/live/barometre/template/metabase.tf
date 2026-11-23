# Metabase of the environment: a Serverless Container plus its application database on this target's managed
# Postgres instance (barometre_rdb). Prod was created by hand in the Scaleway console and is imported (see
# infrastructure/Readme.md, "Metabase"); dev is created here.
#
# In prod the container namespace is shared with Label Studio and other containers, so it is looked up, not managed;
# dev gets its own (manage_container_namespace).

data "scaleway_container_namespace" "existing" {
  count      = var.manage_container_namespace ? 0 : 1
  name       = var.container_namespace_name
  project_id = scaleway_account_project.project.id
}

resource "scaleway_container_namespace" "metabase" {
  count      = var.manage_container_namespace ? 1 : 0
  name       = var.container_namespace_name
  project_id = scaleway_account_project.project.id
}

locals {
  metabase_namespace_id = var.manage_container_namespace ? scaleway_container_namespace.metabase[0].id : data.scaleway_container_namespace.existing[0].id
}

# --- Application database ---

resource "scaleway_rdb_database" "metabase" {
  instance_id = scaleway_rdb_instance.barometre_rdb.id
  name        = var.metabase_db_name
}

# Owner of the database. In prod it is a Scaleway admin user as created by hand (it can create databases and roles):
# reduce that separately, not as part of the import.
resource "scaleway_rdb_user" "metabase" {
  instance_id = scaleway_rdb_instance.barometre_rdb.id
  name        = var.metabase_db_user
  password    = var.metabase_db_password
  is_admin    = var.metabase_db_user_is_admin

  # The API never returns the password: after an import the state has none, and without this the first apply
  # would reset the password to the variable. Remove it to manage rotation from here.
  lifecycle {
    ignore_changes = [password]
  }
}

resource "scaleway_rdb_privilege" "metabase" {
  instance_id   = scaleway_rdb_instance.barometre_rdb.id
  user_name     = scaleway_rdb_user.metabase.name
  database_name = scaleway_rdb_database.metabase.name
  permission    = "all"
}

# --- Container ---

resource "scaleway_container" "metabase" {
  name         = var.metabase_container_name
  namespace_id = local.metabase_namespace_id

  image                  = var.metabase_image
  port                   = 3000
  protocol               = "http1"
  privacy                = "public"
  https_connections_only = false
  sandbox                = "v1"
  deploy                 = true

  # One instance: Metabase keeps state in memory and is not designed to scale out behind this setup
  min_scale = var.metabase_min_scale
  max_scale = 1
  timeout   = 300

  cpu_limit           = var.metabase_cpu_limit    # mVCPU
  memory_limit        = var.metabase_memory_limit # MB
  local_storage_limit = 1000                      # MB

  scaling_option {
    concurrent_requests_threshold = 80
  }

  liveness_probe {
    tcp               = true
    interval          = "10s"
    timeout           = "1s"
    failure_threshold = 30
  }

  environment_variables = {
    "JAVA_OPTS"     = var.metabase_java_opts
    "JAVA_TIMEZONE" = "Europe/Paris"
    "MB_DB_TYPE"    = "postgres"
  }

  secret_environment_variables = {
    "MB_DB_DBNAME" = scaleway_rdb_database.metabase.name
    "MB_DB_HOST"   = scaleway_rdb_instance.barometre_rdb.endpoint_ip
    "MB_DB_PASS"   = var.metabase_db_password
    "MB_DB_PORT"   = tostring(scaleway_rdb_instance.barometre_rdb.endpoint_port)
    "MB_DB_USER"   = scaleway_rdb_user.metabase.name
  }

  # Scaleway never returns secret values, so after an import the state has none and the first apply would rewrite
  # them (and restart Metabase) with the values computed above. Keep this until the plan confirms they match the
  # deployed ones, then remove it to manage the secrets from here.
  # `deploy` is not returned by the API either: without this, the first plan after an import shows `+ deploy = true`
  # and applying it would redeploy (restart) Metabase for no change. A real change (new image...) still deploys.
  lifecycle {
    ignore_changes = [secret_environment_variables, deploy]
  }
}
