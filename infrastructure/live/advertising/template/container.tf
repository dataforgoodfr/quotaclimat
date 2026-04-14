#  Get VPC
data "scaleway_vpc" "default_barometre" {
  name       = "default"
  project_id = data.scaleway_account_project.barometre.id
  is_default = true
}

data "scaleway_vpc_private_network" "pvn_barometre" {
  name   = "pvn-barometre"
  vpc_id = data.scaleway_vpc.default_barometre.id
}

resource "scaleway_container_namespace" "container_namespace" {
  name       = "labelstudio-advertising-${var.environment}-containers"
  project_id = data.scaleway_account_project.project.id
}

resource "scaleway_container" "labelstudio_container" {
  name           = "labelstudio-advertising-${var.environment}"
  namespace_id   = data.scaleway_container_namespace.container_namespace.id
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
    "AWS_S3_ENDPOINT" = "https://s3.fr-par.scw.cloud"

    "CSRF_TRUSTED_ORIGINS" = join(
      "",
      [
        "https://",
        substr(split("/", data.scaleway_container_namespace.container_namespace.registry_endpoint)[1], 7, 50),
        "-labelstudio-advertising-${var.environment}.functions.fnc.fr-par.scw.cloud"
      ]
    )

    "DJANGO_DB"                                = "default"
    "LABEL_STUDIO_DISABLE_SIGNUP_WITHOUT_LINK" = true
    "LABEL_STUDIO_ENABLE_LEGACY_API_TOKEN"     = true
    "DEBUG"                                    = "true"

    "LABEL_STUDIO_HOST" = join(
      "",
      [
        "https://",
        substr(split("/", data.scaleway_container_namespace.container_namespace.registry_endpoint)[1], 7, 50),
        "-labelstudio-advertising-${var.environment}.functions.fnc.fr-par.scw.cloud"
      ]
    )

    "LABEL_STUDIO_S3_DEBUG"        = true
    "LABEL_STUDIO_TASKS_PAGE_SIZE" = 10
    "LABEL_STUDIO_USERNAME"        = "admin@labelstudio.com"
    "LOG_LEVEL"                    = "INFO"
    "POSTGRE_NAME"                 = scaleway_rdb_database.labelstudio_db.name
    "POSTGRE_PORT"                 = data.scaleway_rdb_instance.barometre_rdb.endpoint_port
    "SSRF_PROTECTION_ENABLED"      = true

  }
  secret_environment_variables = {
    "AWS_ACCESS_KEY_ID"       = scaleway_iam_api_key.project_api_key.access_key
    "AWS_SECRET_ACCESS_KEY"   = scaleway_iam_api_key.project_api_key.secret_key
    "LABEL_STUDIO_PASSWORD"   = var.labelstudio_admin_password
    "LABEL_STUDIO_USER_TOKEN" = var.labelstudio_user_token
    "POSTGRE_PASSWORD"        = scaleway_rdb_user.labelstudio_user.password
    "POSTGRE_USER"            = scaleway_rdb_user.labelstudio_user.name
    "POSTGRE_HOST"            = data.scaleway_rdb_instance.barometre_rdb.endpoint_ip
  }
}
