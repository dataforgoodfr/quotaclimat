resource "scaleway_container_namespace" "container_namespace" {
  name       = "orchestrator-${var.environment}-containers"
  project_id = data.scaleway_account_project.barometre.id
}

resource "scaleway_container" "kestra" {
  name           = "kestra-orchestrator-${var.environment}"
  namespace_id   = scaleway_container_namespace.container_namespace.id
  min_scale      = var.environment == "prod" ? 1 : 0
  max_scale      = 1
  memory_limit_bytes = 4096000000
  cpu_limit      = 2000
  image          = "docker.io/kestra/kestra:${var.kestra_image_tag}"
  command        = ["/app/kestra", "server", "standalone"]
  port           = 8080
  protocol       = "http1"

  sandbox = "v1"

  scaling_option {
    concurrent_requests_threshold = 20
  }

  environment_variables = {
    "JAVA_OPTS"            = "-Xms512m -Xmx3g"
    "KESTRA_CONFIGURATION" = <<-YAML
      micronaut:
        server:
          port: 8080
        management:
          endpoints:
            web:
              exposure:
                include: health
              port: 8080
      datasources:
        postgres:
          url: jdbc:postgresql://${data.scaleway_rdb_instance.barometre_rdb.endpoint_ip}:${data.scaleway_rdb_instance.barometre_rdb.endpoint_port}/${scaleway_rdb_database.kestra_db.name}
          driverClassName: org.postgresql.Driver
          username: ${scaleway_rdb_user.kestra_user.name}
          password: ${scaleway_rdb_user.kestra_user.password}
      kestra:
        repository:
          type: postgres
        queue:
          type: postgres
        storage:
          type: s3
          s3:
            accessKey: "${scaleway_iam_api_key.orchestrator_api_key.access_key}"
            secretKey: "${scaleway_iam_api_key.orchestrator_api_key.secret_key}"
            region: "fr-par"
            bucket: "${scaleway_object_bucket.kestra_storage.name}"
            endpoint: "https://s3.fr-par.scw.cloud"
            forcePathStyle: "true"
        server:
          basicAuth:
            enabled: true
            username: "${var.kestra_admin_email}"
            password: "${var.kestra_admin_password}"
        tasks:
          tmpDir:
            path: /tmp/kestra-wd/tmp
    YAML
  }

  secret_environment_variables = {
    "SECRET_S3_ACCESS_KEY" = scaleway_iam_api_key.orchestrator_api_key.access_key
    "SECRET_S3_SECRET_KEY" = scaleway_iam_api_key.orchestrator_api_key.secret_key
  }

  depends_on = [
    scaleway_rdb_privilege.kestra_privilege,
    scaleway_object_bucket.kestra_storage,
  ]
}
