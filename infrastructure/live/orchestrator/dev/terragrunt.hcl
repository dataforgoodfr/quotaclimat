include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../template"
}

inputs = {
  environment                = "dev"
  barometre_project_name     = "barometre-dev"
  barometre_pg_instance_name = get_env("TF_VAR_barometre_pg_instance_name", "rdb-poc")
  kestra_db_password         = get_env("TF_VAR_kestra_db_password")
  kestra_admin_email         = get_env("TF_VAR_kestra_admin_email", "admin@kestra.io")
  kestra_admin_password      = get_env("TF_VAR_kestra_admin_password")
  policy_project_names       = ["rrs-dev", "barometre-dev", "advertising-dev", "climate-safeguards-dev"]
}
