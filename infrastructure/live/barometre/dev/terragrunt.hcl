include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../template"
}

inputs = {
  environment                      = "dev"
  postgres_admin_user               = get_env("TF_VAR_postgres_admin_user")
  postgres_admin_password           = get_env("TF_VAR_postgres_admin_password")
  postgres_admin_password_version   = 1
  node_type                         = "DB-DEV-S"
  volume_type                       = "sbs_5k"
  volume_size_in_gb                 = 100
}
