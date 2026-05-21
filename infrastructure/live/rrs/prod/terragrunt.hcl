include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../template"
}

inputs = {
  environment                      = "prod"
  postgres_admin_password          = get_env("TF_VAR_postgres_admin_password")
  postgres_admin_password_version  = 1
  postgres_job_password            = get_env("TF_VAR_postgres_job_password")
  node_type                        = "DB-DEV-S"
  volume_type                      = "sbs_5k"
  volume_size_in_gb                = 30

  # Container registry / jobs
  image_tag = "prod"

  # LLM API keys
  mistral_api_key   = get_env("TF_VAR_mistral_api_key")
  anthropic_api_key = get_env("TF_VAR_anthropic_api_key")

  # Barometre rrs-read user
  barometre_rrs_read_password = get_env("TF_VAR_barometre_rrs_read_password")

  # S3 / Scaleway Object Storage
  bucket_name   = get_env("TF_VAR_bucket_name")

  # In prod, restrict DB access to Scaleway serverless job ranges only (no 0.0.0.0/0).
  acl_allowed_ips = []
}
