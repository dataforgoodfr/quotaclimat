locals {
  image_uri = "${scaleway_registry_namespace.rrs.endpoint}/rrs-base:${var.image_tag}"

  rrs_pg_env = {
    RRS_PG_HOST     = scaleway_rdb_instance.rrs_rdb.endpoint_ip
    RRS_PG_PORT     = tostring(scaleway_rdb_instance.rrs_rdb.endpoint_port)
    RRS_PG_DATABASE = scaleway_rdb_database.rrs.name
    RRS_PG_USER     = scaleway_rdb_user.rrs_job_user.name
    PYTHONPATH      = "/app"
  }

  rrs_pg_secret_env = {
    RRS_PG_PASSWORD = scaleway_rdb_user.rrs_job_user.password
  }

  barometre_env = {
    POSTGRES_HOST = var.barometre_postgres_host
    POSTGRES_PORT = tostring(var.barometre_postgres_port)
    POSTGRES_DB   = var.barometre_postgres_db
    POSTGRES_USER = var.barometre_postgres_user
  }

  barometre_secret_env = {
    POSTGRES_PASSWORD = scaleway_secret_version.barometre_postgres_password.data
  }
}

resource "scaleway_serverless_job" "rrs_clustering" {
  name         = "rrs-clustering-${var.environment}"
  cpu_limit    = var.job_cpu_limit_clustering
  memory_limit = var.job_memory_limit_clustering
  image_uri    = local.image_uri
  project_id   = scaleway_account_project.project.id
  region       = "fr-par"
  timeout      = var.job_timeout

  command = ["python", "-m", "rrs.clustering.main"]

  environment_variables = merge(local.rrs_pg_env, {
    SPACY_MODEL             = var.clustering_spacy_model
    WINDOW_SIZE             = tostring(var.clustering_window_size)
    OVERLAP_TOKENS          = tostring(var.clustering_overlap_tokens)
    PROVIDER                = var.clustering_provider
    MAX_CONCURRENT_REQUESTS = tostring(var.clustering_max_concurrent_requests)
    MERGE_BATCH_SIZE        = tostring(var.clustering_merge_batch_size)
    MERGE_MAX_ROUNDS        = tostring(var.clustering_merge_max_rounds)
    TARGET_CLUSTERS         = tostring(var.clustering_target_clusters)
    MIN_CLUSTERS            = tostring(var.clustering_min_clusters)
    MAX_CLUSTERS            = tostring(var.clustering_max_clusters)
    CLUSTER_SCALE           = tostring(var.clustering_cluster_scale)
    LOW_THRESHOLD           = tostring(var.clustering_low_threshold)
    HIGH_THRESHOLD          = tostring(var.clustering_high_threshold)
    EMBEDDING_BACKEND       = var.clustering_embedding_backend
    EMBEDDING_MODEL         = var.clustering_embedding_model
    EXPIRY_DAYS             = tostring(var.clustering_expiry_days)
  })

  secret_environment_variables = merge(local.rrs_pg_secret_env, {
    MISTRAL_API_KEY   = scaleway_secret_version.mistral_api_key.data
    ANTHROPIC_API_KEY = scaleway_secret_version.anthropic_api_key.data
  })
}

resource "scaleway_serverless_job" "rrs_import_segments" {
  name         = "rrs-import-segments-${var.environment}"
  cpu_limit    = var.job_cpu_limit_import
  memory_limit = var.job_memory_limit_import
  image_uri    = local.image_uri
  project_id   = scaleway_account_project.project.id
  region       = "fr-par"
  timeout      = var.job_timeout

  command = ["python", "-m", "rrs.keyword_detection.import_segments"]

  environment_variables = merge(local.rrs_pg_env, local.barometre_env, {
    BUCKET_NAME = var.bucket_name
  })

  secret_environment_variables = merge(local.rrs_pg_secret_env, local.barometre_secret_env)
}

resource "scaleway_serverless_job" "rrs_import_cases" {
  name         = "rrs-import-claims-${var.environment}"
  cpu_limit    = var.job_cpu_limit_import
  memory_limit = var.job_memory_limit_import
  image_uri    = local.image_uri
  project_id   = scaleway_account_project.project.id
  region       = "fr-par"
  timeout      = var.job_timeout

  # docker-compose entrypoint: rrs.keyword_detection.import_cases
  command = ["python", "-m", "rrs.keyword_detection.import_cases"]

  environment_variables = merge(local.rrs_pg_env, local.barometre_env)

  secret_environment_variables = merge(local.rrs_pg_secret_env, local.barometre_secret_env)
}
