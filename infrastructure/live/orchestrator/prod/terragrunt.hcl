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
  postgres_password_windmill  = get_env("TF_VAR_postgres_password_windmill")

  # Makes the windmill user a Scaleway admin while it creates its database and roles (template/grants.tf).
  # TF_VAR_windmill_bootstrap=true for the first apply and before a destroy only.
  windmill_bootstrap = get_env("TF_VAR_windmill_bootstrap", "false") == "true"

  # IAM user IDs (JSON list) allowed to administer the GlitchTip bucket, e.g.
  # '["<user-id>"]'. Get yours with: scw iam api-key get <SCW_ACCESS_KEY> (user_id).
  glitchtip_bucket_admin_user_ids = jsondecode(get_env("TF_VAR_glitchtip_bucket_admin_user_ids"))
}
