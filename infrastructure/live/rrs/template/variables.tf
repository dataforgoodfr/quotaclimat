variable "environment" {
  type = string
}

variable "postgres_admin_password" {
  type      = string
  sensitive = true
}

variable "postgres_admin_password_version" {
  type    = number
  default = 1
}

variable "postgres_job_password" {
  type      = string
  sensitive = true
}

variable "postgres_migrate_password" {
  type      = string
  sensitive = true
}
variable "db_metabase_user_password" {
  type      = string
  sensitive = true
}

variable "acl_allowed_ips" {
  type = list(object({
    ip          = string
    description = string
  }))
  default     = []
  description = "List of CIDRs allowed to reach the database. Ignored in dev (0.0.0.0/0 is used instead)."
}

variable "node_type" {
  type    = string
  default = "DB-DEV-S"
}

variable "volume_type" {
  type    = string
  default = "sbs_5k"
}

variable "volume_size_in_gb" {
  type    = number
  default = 20
}

# Container registry / jobs

variable "image_tag" {
  type        = string
  default     = "latest"
  description = "Tag of the rrs-base image in the container registry."
}

variable "job_cpu_limit_clustering" {
  type        = number
  default     = 2240
  description = "CPU limit for the clustering job in millicores."
}

variable "job_memory_limit_clustering" {
  type        = number
  default     = 4096
  description = "Memory limit for the clustering job in MiB."
}

variable "job_cpu_limit_import" {
  type        = number
  default     = 1120
  description = "CPU limit for the import jobs in millicores."
}

variable "job_memory_limit_import" {
  type        = number
  default     = 2048
  description = "Memory limit for the import jobs in MiB."
}

variable "job_timeout" {
  type        = string
  default     = "3600s"
  description = "Maximum execution duration for all jobs."
}

# LLM API keys (clustering job)

variable "mistral_api_key" {
  type      = string
  sensitive = true
}

variable "anthropic_api_key" {
  type      = string
  sensitive = true
}

variable "barometre_rrs_read_password" {
  type        = string
  sensitive   = true
  description = "Password for the rrs-read-{env} user on the barometre database."
}

# S3 / Scaleway Object Storage (import_segments job)

variable "bucket_name" {
  type        = string
  description = "Scaleway bucket name for keyword detection data."
}

# Clustering tunables (sensible defaults from docker-compose)

variable "clustering_spacy_model" {
  type    = string
  default = "fr_core_news_sm"
}

variable "clustering_window_size" {
  type    = number
  default = 3
}

variable "clustering_overlap_tokens" {
  type    = number
  default = 30
}

variable "clustering_provider" {
  type    = string
  default = "anthropic"
}

variable "clustering_max_concurrent_requests" {
  type    = number
  default = 5
}

variable "clustering_merge_batch_size" {
  type    = number
  default = 30
}

variable "clustering_merge_max_rounds" {
  type    = number
  default = 5
}

variable "clustering_target_clusters" {
  type    = number
  default = 5
}

variable "clustering_min_clusters" {
  type    = number
  default = 7
}

variable "clustering_max_clusters" {
  type    = number
  default = 150
}

variable "clustering_cluster_scale" {
  type    = number
  default = 1.0
}

variable "clustering_low_threshold" {
  type    = number
  default = 0.4
}

variable "clustering_high_threshold" {
  type    = number
  default = 0.85
}

variable "clustering_embedding_backend" {
  type    = string
  default = "mistral"
}

variable "clustering_embedding_model" {
  type    = string
  default = "dangvantuan/sentence-camembert-large"
}

variable "clustering_expiry_days" {
  type    = number
  default = 30
}
