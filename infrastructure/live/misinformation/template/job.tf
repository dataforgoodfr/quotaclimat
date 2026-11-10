# label-misinformation job, one per country (image built by
# .github/workflows/label-misinformation.yml).
resource "scaleway_job_definition" "main" {
  for_each = var.countries

  name                   = "label-misinformation-${each.key}-${var.environment}"
  cpu_limit              = 4000
  memory_limit           = 8192
  local_storage_capacity = 4096
  image_uri              = "rg.fr-par.scw.cloud/misinformation/label-misinformation:${var.image_tag}"
  timeout                = "60m"
  project_id             = scaleway_account_project.project.id

  cron {
    schedule = each.value.job_cron_schedule
    timezone = each.value.job_cron_schedule_timezone
  }

  env = {
    "APP_NAME"                = "label-misinformation-${each.key}-${var.environment}"
    "AWS_DEFAULT_REGION"      = "fr-par"
    "BUCKET_INPUT"            = "mediatree"
    "BUCKET_OUTPUT"           = scaleway_object_bucket.bucket[each.key].name
    "BUCKET_OUTPUT_FOLDER"    = "label-misinformation-input"
    "COUNTRY"                 = each.key
    "ENV"                     = "production" # maybe this should be equal to var.environment.. does not work for now
    "KEYWORDS_URL"            = "https://keywords.mediatree.fr/api/export/single"
    "LABEL_STUDIO_PROJECT"    = each.value.labelstudio_project
    "LABEL_STUDIO_PROJECT_ID" = each.value.labelstudio_project_id
    "LABEL_STUDIO_URL"        = "https://${scaleway_container.labelstudio_container[each.key].domain_name}"
    "LLM_PROVIDER"            = each.value.llm_provider
    "LOGLEVEL"                = "INFO"
    "MEDIATREE_AUTH_URL"      = "https://keywords.mediatree.fr/api/auth/token/"
    "MEDIATREE_USER"          = "quotaclimat"
    "MODEL_NAME"              = each.value.model_name
    "MODIN_MEMORY"            = 3000000000
    "POSTGRES_DB"             = data.scaleway_rdb_database.barometre.name
    "POSTGRES_HOST"           = data.scaleway_rdb_instance.barometre_rdb.endpoint_ip
    "POSTGRES_PORT"           = data.scaleway_rdb_instance.barometre_rdb.endpoint_port
    "POSTGRES_USER"           = "barometreclimat"
    "RAY_COLOR_PREFIX"        = 1
    "SENTRY_DSN"              = var.sentry_dsn
  }

  # Per-country secrets
  secret_reference {
    secret_id   = scaleway_secret.secret_label_studio_token[each.key].id
    environment = "API_LABEL_STUDIO_KEY"
  }
  secret_reference {
    secret_id   = scaleway_secret.access_key_id[each.key].id
    environment = "BUCKET_ACCESS"
  }
  secret_reference {
    secret_id   = scaleway_secret.secret_key_id[each.key].id
    environment = "BUCKET_SECRET"
  }
  # Shared secrets
  secret_reference {
    secret_id   = scaleway_secret.mediatree_password.id
    environment = "MEDIATREE_PASSWORD"
  }
  secret_reference {
    secret_id   = scaleway_secret.openai_api_key.id
    environment = "OPENAI_API_KEY"
  }
  secret_reference {
    secret_id   = scaleway_secret.mistral_api_key.id
    environment = "MISTRAL_API_KEY"
  }
  secret_reference {
    secret_id   = scaleway_secret.postgres_password_barometre.id
    environment = "POSTGRES_PASSWORD"
  }

  depends_on = [scaleway_container.labelstudio_container]
}
