include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../template"
}

inputs = {
  environment                = "prod"
  labelstudio_admin_password = get_env("TF_VAR_labelstudio_admin_password")
  labelstudio_user_token     = get_env("TF_VAR_labelstudio_user_token")
}
