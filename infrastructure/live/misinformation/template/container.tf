resource "scaleway_container_namespace" "container_namespace" {
  name       = "${var.subject}-safeguards-containers-${var.environment}"
  project_id = scaleway_account_project.project.id
}

locals {
  # https://<namespace-prefix>-labelstudio-<country>-<env>.functions.fnc.fr-par.scw.cloud
  labelstudio_host = {
    for country in keys(var.countries) : country => join(
      "",
      [
        "https://",
        substr(split("/", scaleway_container_namespace.container_namespace.registry_endpoint)[1], 7, 50),
        "-labelstudio-${country}-${var.environment}.functions.fnc.fr-par.scw.cloud"
      ]
    )
  }
}

resource "scaleway_container" "labelstudio_container" {
  for_each = var.countries

  name           = "labelstudio-${each.key}-${var.environment}"
  namespace_id   = scaleway_container_namespace.container_namespace.id
  min_scale      = 0
  max_scale      = 1
  memory_limit   = 2048
  cpu_limit      = 1000
  registry_image = "heartexlabs/label-studio:1.22.0"
  port           = 8080
  deploy         = true
  protocol       = "http1"

  sandbox = "v1"

  scaling_option {
    concurrent_requests_threshold = 80
  }
  health_check {
    http {
      path = "/health"
    }
    interval          = "45s"
    failure_threshold = 11
  }

  environment_variables = {
    "AWS_S3_ENDPOINT"                          = "https://s3.fr-par.scw.cloud"
    "CSRF_TRUSTED_ORIGINS"                     = local.labelstudio_host[each.key]
    "DJANGO_DB"                                = "default"
    "LABEL_STUDIO_DISABLE_SIGNUP_WITHOUT_LINK" = true
    "LABEL_STUDIO_ENABLE_LEGACY_API_TOKEN"     = true
    "DEBUG"                                    = "true"
    "LABEL_STUDIO_HOST"                        = local.labelstudio_host[each.key]
    "LABEL_STUDIO_S3_DEBUG"                    = true
    "LABEL_STUDIO_TASKS_PAGE_SIZE"             = 10
    "LABEL_STUDIO_USERNAME"                    = "admin@labelstudio.com"
    "LOG_LEVEL"                                = "INFO"
    "POSTGRE_NAME"                             = scaleway_rdb_database.labelstudio_db[each.key].name
    "POSTGRE_PORT"                             = data.scaleway_rdb_instance.barometre_rdb.endpoint_port
    "SSRF_PROTECTION_ENABLED"                  = true
  }
  secret_environment_variables = {
    "AWS_ACCESS_KEY_ID"       = scaleway_iam_api_key.project_api_key[each.key].access_key
    "AWS_SECRET_ACCESS_KEY"   = scaleway_iam_api_key.project_api_key[each.key].secret_key
    "LABEL_STUDIO_PASSWORD"   = var.country_secrets[each.key].labelstudio_admin_password
    "LABEL_STUDIO_USER_TOKEN" = var.country_secrets[each.key].labelstudio_user_token
    "POSTGRE_PASSWORD"        = scaleway_rdb_user.labelstudio_user[each.key].password
    "POSTGRE_USER"            = scaleway_rdb_user.labelstudio_user[each.key].name
    "POSTGRE_HOST"            = data.scaleway_rdb_instance.barometre_rdb.endpoint_ip
  }
}
