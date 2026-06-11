# --- VM ---

variable "instance_type" {
  type    = string
  default = "DEV1-M"
}

variable "instance_image" {
  type        = string
  default     = "ubuntu_noble"
  description = "Scaleway image label (e.g. ubuntu_noble, debian_bookworm)."
}

variable "data_volume_size" {
  type        = number
  default     = 100
  description = "Size in GB of the block storage volume for Docker data."
}

# --- Managed Postgres ---

variable "pg_instance_name" {
  type        = string
  description = "Name of the existing Scaleway managed PostgreSQL instance to create databases in."
}

variable "pg_project_name" {
  type        = string
  description = "Name of the Scaleway project that owns the managed PG instance."
}

variable "postgres_password_kestra" {
  type      = string
  sensitive = true
}

variable "postgres_password_glitchtip" {
  type      = string
  sensitive = true
}
