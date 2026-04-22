include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../template"
}

inputs = {
  environment             = "dev"
  postgres_admin_user      = get_env("TF_VAR_postgres_admin_user")
  postgres_admin_password  = get_env("TF_VAR_postgres_admin_password")
  barometre_admin_password = get_env("TF_VAR_barometre_admin_password")
}
