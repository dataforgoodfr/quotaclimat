include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../template"
}

inputs = {
  postgres_admin_password       = get_env("TF_VAR_postgres_admin_password")
  postgres_admin_user           = get_env("TF_VAR_postgres_admin_user")
  dgccrf_user_password          = get_env("TF_VAR_dgccrf_user_password")
  postgres_db_name              = get_env("TF_VAR_postgres_db_name")
  barometre_pg_instance_name    = get_env("TF_VAR_barometre_pg_instance_name")
}
