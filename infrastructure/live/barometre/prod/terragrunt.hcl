include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../template"
}

inputs = {
  environment = "prod"

  # Prod reuses the legacy shared project/instance (created by hand in 2023),
  # not a dedicated per-env pair like dev — see `scw account project list`
  # and `scw rdb instance list`.
  project_name      = "barometre"
  rdb_instance_name = "rdb-poc"

  # Matches the real instance: backups are enabled, unlike the dev default.
  disable_backup = false

  node_type         = "db-dev-l"
  volume_type       = "sbs_5k"
  volume_size_in_gb = 100

  # Instance root admin (set at instance creation) is admin_paul.
  # Set TF_VAR_postgres_admin_user=admin_paul before plan/import.
  postgres_admin_user             = get_env("TF_VAR_postgres_admin_user")
  postgres_admin_password         = get_env("TF_VAR_postgres_admin_password")
  postgres_admin_password_version = 1

  # Database-level admin (permission="all" on barometre per `scw rdb privilege list`)
  # is barometreclimat — distinct from admin_paul, whose superuser access on
  # barometre shows up as permission="custom", not "all".
  # Set TF_VAR_database_admin_user=barometreclimat before plan/import.
  database_admin_user     = get_env("TF_VAR_database_admin_user")
  database_admin_password = get_env("TF_VAR_database_admin_password")

  barometre_rrs_read_password = get_env("TF_VAR_barometre_rrs_read_password")
  metabase_analytics_password = get_env("TF_VAR_metabase_analytics_password")

  # Metabase: created by hand in the console, described in template/metabase.tf so that it can be imported (see
  # "Metabase" in infrastructure/Readme.md). Every value is the deployed one, so that the import shows no change.
  manage_container_namespace = false # shared with Label Studio and other containers: looked up
  container_namespace_name   = "barometre"
  metabase_container_name    = "metabase-barometre"
  metabase_cpu_limit         = 3000
  metabase_memory_limit      = 8192
  metabase_min_scale         = 1
  metabase_java_opts         = "-Xmx8g"
  metabase_db_user           = "metabase_barometre"
  metabase_db_user_is_admin  = true
  # Password of the Metabase database user (the value of MB_DB_PASS in the container)
  metabase_db_password = get_env("TF_VAR_metabase_db_password")

  # Hand-curated personal/CI IPs currently allowed on rdb-poc, on top of the
  # Scaleway job CIDRs already baked into the template. Pruning this list of
  # stale personal IPs is a separate follow-up, not part of the import.
  # JSON-encoded list of {ip, description} objects — see .env.secrets.dist.
  acl_allowed_ips = get_env("TF_VAR_acl_allowed_ips")
}
