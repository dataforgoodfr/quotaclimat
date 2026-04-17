include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../template"
}

inputs = {
  environment                   = "dev"
  labelstudio_admin_password    = get_env("TF_VAR_labelstudio_admin_password")
  labelstudio_user_token        = get_env("TF_VAR_labelstudio_user_token")
  postgres_password_labelstudio = get_env("TF_VAR_postgres_password_labelstudio")
  barometre_pg_instance_name    = get_env("TF_VAR_barometre_pg_instance_name", "rdb-poc")
}
