variable "environment" {
  type = string
}

variable "project_name" {
  type = string
}

variable "rdb_instance_name" {
  type = string
}

variable "disable_backup" {
  type    = bool
  default = true
}

variable "postgres_admin_user" {
  type        = string
  sensitive   = true
  description = "Instance-level root admin user_name, set at instance creation. Superuser access is implicit and not represented as a database-level privilege grant."
}

variable "postgres_admin_password" {
  type      = string
  sensitive = true
}

variable "database_admin_user" {
  type        = string
  sensitive   = true
  description = "User granted permission='all' on the barometre database. Distinct from postgres_admin_user, which is the instance's root superuser."
}

variable "database_admin_password" {
  type        = string
  sensitive   = true
  description = "Password for database_admin_user. Only used when database_admin_user differs from postgres_admin_user (e.g. prod's barometreclimat vs admin_paul) — dev reuses the instance root admin, which already exists."
  default     = ""
}
variable "barometre_rrs_read_password" {
  type      = string
  sensitive = true
}

variable "metabase_analytics_password" {
  type      = string
  sensitive = true
}

variable "postgres_admin_password_version" {
  type    = number
  default = 1
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
  default = 10
}

variable "acl_allowed_ips" {
  type        = string
  sensitive   = true
  default     = "[]"
  description = "JSON-encoded list of {ip, description} objects allowed to reach the database. Ignored in dev (0.0.0.0/0 is used instead)."
}

# --- GPU instance ---

variable "multi_zone" {
  type        = bool
  default     = false
  description = "If true, provisions a second, independent GPU instance in fr-par-2 alongside the primary one in gpu_zone. If false, only the primary GPU instance is created."
}

variable "gpu_zone" {
  type        = string
  default     = "fr-par-1"
  description = "Scaleway zone for the GPU instance (must stock the L4 GPU offer)."
}

variable "gpu_instance_type" {
  type        = string
  default     = "L4-1-24G"
  description = "Scaleway GPU instance commercial type."
}

variable "gpu_image_label" {
  type        = string
  default     = "ubuntu_jammy_gpu_os_12"
  description = "Marketplace image label compatible with GPU instances (bundles NVIDIA drivers)."
}

variable "gpu_root_volume_size_in_gb" {
  type    = number
  default = 100
}


# --- Metabase (metabase.tf) ---

variable "manage_container_namespace" {
  type        = bool
  default     = false
  description = "false: the Serverless Containers namespace exists and is shared with other containers, so it is looked up (prod). true: this target creates it (dev)."
}

variable "container_namespace_name" {
  type        = string
  description = "Name of the Serverless Containers namespace of Metabase, looked up or created depending on manage_container_namespace."
}

variable "metabase_container_name" {
  type        = string
  description = "Name of the Metabase container (prod: metabase-barometre, the name it was created with by hand)."
}

variable "metabase_image" {
  type        = string
  default     = "metabase/metabase:v0.64.2.2"
  description = "Metabase image (Docker Hub). Changing it redeploys the container."
}

variable "metabase_cpu_limit" {
  type        = number
  description = "CPU limit of the Metabase container, in mVCPU."
}

variable "metabase_memory_limit" {
  type        = number
  description = "Memory limit of the Metabase container, in MB."
}

variable "metabase_min_scale" {
  type        = number
  default     = 1
  description = "Minimum number of instances. 0 lets the container sleep (cold start of about a minute for Metabase)."
}

variable "metabase_java_opts" {
  type        = string
  description = "JAVA_OPTS of the Metabase JVM, e.g. -Xmx8g. Keep the heap below the container memory limit."
}

variable "metabase_db_name" {
  type    = string
  default = "metabase"
}

variable "metabase_db_user" {
  type        = string
  description = "Owner of the Metabase database (prod: metabase_barometre, as created by hand)."
}

variable "metabase_db_user_is_admin" {
  type        = bool
  default     = false
  description = "Scaleway admin user (CREATEDB + CREATEROLE). Metabase does not need it: true only for the prod user, which was created that way by hand."
}

variable "metabase_db_password" {
  type        = string
  sensitive   = true
  description = "Password of the Metabase database user, also the MB_DB_PASS secret of the container."
}
