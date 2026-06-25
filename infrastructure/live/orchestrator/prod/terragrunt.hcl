include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../template"
}

inputs = {
  # Elastic Metal server defaults (offer/zone/os) live in variables.tf; override here if needed.

  # Managed Postgres (reuse existing barometre instance)
  pg_instance_name = get_env("TF_VAR_pg_instance_name", "rdb-poc")
  pg_project_name  = get_env("TF_VAR_pg_project_name", "barometre")

  # DB passwords
  postgres_password_kestra    = get_env("TF_VAR_postgres_password_kestra")
  postgres_password_glitchtip = get_env("TF_VAR_postgres_password_glitchtip")
}
