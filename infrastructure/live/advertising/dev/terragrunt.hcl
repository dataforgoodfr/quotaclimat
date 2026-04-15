include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../template"
}

inputs = {
  environment                = "dev"
  labelstudio_admin_password = get_env("TF_VAR_labelstudio_admin_password")
  labelstudio_user_token     = get_env("TF_VAR_labelstudio_user_token")
  labelstudio_user_token     = get_env("postgres_password_labelstudio")
  labelstudio_user_token     = get_env("barometre_pg_instance_name")
}
