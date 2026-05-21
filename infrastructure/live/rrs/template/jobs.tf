locals {
  image_uri = "${scaleway_registry_namespace.rrs.endpoint}/rrs-base:${var.image_tag}"

  rrs_pg_env = {
    RRS_PG_HOST     = try(scaleway_rdb_instance.rrs_rdb.load_balancer[0].ip, null)
    RRS_PG_PORT     = try(tostring(scaleway_rdb_instance.rrs_rdb.load_balancer[0].port), null)
    RRS_PG_DATABASE = scaleway_rdb_database.rrs.name
    RRS_PG_USER     = scaleway_rdb_user.rrs_job_user.name
    PYTHONPATH      = "/app"
  }

  barometre_env = {
    POSTGRES_HOST = try(data.scaleway_rdb_instance.barometre.load_balancer[0].ip, null)
    POSTGRES_PORT = try(tostring(data.scaleway_rdb_instance.barometre.load_balancer[0].port), null)
    POSTGRES_DB   = data.scaleway_rdb_database.barometre.name
    POSTGRES_USER = scaleway_rdb_user.rrs_read.name
  }
}

resource "scaleway_job_definition" "rrs_clustering" {
  name                    = "rrs-clustering-${var.environment}"
  cpu_limit               = var.job_cpu_limit_clustering
  memory_limit            = var.job_memory_limit_clustering
  image_uri               = local.image_uri
  project_id              = scaleway_account_project.project.id
  region                  = "fr-par"
  timeout                 = var.job_timeout
  local_storage_capacity  = 1024

  startup_command = ["python", "-m", "rrs.clustering.main"]

  env = merge(local.rrs_pg_env, {
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

  secret_reference {
    secret_id   = scaleway_secret.rrs_job_password.id
    environment = "RRS_PG_PASSWORD"
  }

  secret_reference {
    secret_id   = scaleway_secret.mistral_api_key.id
    environment = "MISTRAL_API_KEY"
  }

  secret_reference {
    secret_id   = scaleway_secret.anthropic_api_key.id
    environment = "ANTHROPIC_API_KEY"
  }
}

resource "scaleway_job_definition" "rrs_import_segments" {
  name                    = "rrs-import-segments-${var.environment}"
  cpu_limit               = var.job_cpu_limit_import
  memory_limit            = var.job_memory_limit_import
  image_uri               = local.image_uri
  project_id              = scaleway_account_project.project.id
  region                  = "fr-par"
  timeout                 = var.job_timeout
  local_storage_capacity  = 1024


  startup_command = ["python", "-m", "rrs.keyword_detection.import_segments"]

  env = merge(local.rrs_pg_env, local.barometre_env, {
    BUCKET_NAME = var.bucket_name
  })

  secret_reference {
    secret_id   = scaleway_secret.rrs_job_password.id
    environment = "RRS_PG_PASSWORD"
  }

  secret_reference {
    secret_id   = scaleway_secret.barometre_rrs_read_password.id
    environment = "POSTGRES_PASSWORD"
  }

}

resource "scaleway_job_definition" "rrs_import_cases" {
  name                    = "rrs-import-claims-${var.environment}"
  cpu_limit               = var.job_cpu_limit_import
  memory_limit            = var.job_memory_limit_import
  image_uri               = local.image_uri
  project_id              = scaleway_account_project.project.id
  region                  = "fr-par"
  timeout                 = var.job_timeout
  local_storage_capacity  = 1024

  # docker-compose entrypoint: rrs.keyword_detection.import_cases
  startup_command = ["python", "-m", "rrs.keyword_detection.import_cases"]

  env = merge(local.rrs_pg_env, local.barometre_env)

  secret_reference {
    secret_id   = scaleway_secret.rrs_job_password.id
    environment = "RRS_PG_PASSWORD"
  }

  secret_reference {
    secret_id   = scaleway_secret.barometre_rrs_read_password.id
    environment = "POSTGRES_PASSWORD"
  }
}
