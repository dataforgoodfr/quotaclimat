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
  postgres_admin_password_version = 1
  node_type                       = "DB-DEV-S"
  volume_type                     = "sbs_5k"
  volume_size_in_gb               = 20
  # Ignored in dev — 0.0.0.0/0 is used instead (see template/database.tf).
  acl_allowed_ips = "[]"

  # GPU instance defaults (see template/variables.tf) — override here if dev needs something non-default.
  gpu_zone                   = "fr-par-1"
  gpu_instance_type          = "L4-1-24G"
  gpu_image_label            = "ubuntu_jammy_gpu_os_12"
  gpu_root_volume_size_in_gb = 100
}
