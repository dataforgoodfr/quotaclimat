include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../template"
}

inputs = {
  environment             = "dev"
  project_name            = "barometre-dev"
  rdb_instance_name       = "rdb-barometre-dev"
  disable_backup          = true
  postgres_admin_user     = get_env("TF_VAR_postgres_admin_user")
  postgres_admin_password = get_env("TF_VAR_postgres_admin_password")
  # Dev's instance root admin doubles as the database admin (no separate role like prod's barometreclimat).
  database_admin_user             = get_env("TF_VAR_postgres_admin_user")
  database_admin_password         = ""
  barometre_rrs_read_password     = get_env("TF_VAR_barometre_rrs_read_password")
  metabase_analytics_password     = get_env("TF_VAR_metabase_analytics_password")
  postgres_admin_password_version = 1
  node_type                       = "DB-DEV-S"
  volume_type                     = "sbs_5k"
  volume_size_in_gb               = 20
  # Ignored in dev — 0.0.0.0/0 is used instead (see template/database.tf).
  acl_allowed_ips = "[]"

  # Metabase dev: new, created by template/metabase.tf (there is none by hand in barometre-dev).
  manage_container_namespace = true
  container_namespace_name   = "metabase-dev"
  metabase_container_name    = "metabase-dev"
  metabase_cpu_limit         = 1000
  metabase_memory_limit      = 2048
  # Sleeps when unused: the first request after a pause takes about a minute
  metabase_min_scale = 0
  metabase_java_opts = "-Xmx1536m"
  metabase_db_user   = "metabase-dev"
  # Password of the new Metabase database user: choose one
  metabase_db_password = get_env("TF_VAR_metabase_db_password")

  # GPU instance defaults (see template/variables.tf) — override here if dev needs something non-default.
  multi_zone                 = false
  gpu_zone                   = "fr-par-1"
  gpu_instance_type          = "L4-1-24G"
  gpu_image_label            = "ubuntu_jammy_gpu_os_12"
  gpu_root_volume_size_in_gb = 100
}
