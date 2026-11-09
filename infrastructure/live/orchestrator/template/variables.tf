# --- Bare-metal server (Elastic Metal) ---

variable "zone" {
  type        = string
  default     = "fr-par-2"
  description = "Scaleway zone for the Elastic Metal server (must stock the offer)."
}

variable "offer_name" {
  type        = string
  default     = "EM-A610R-NVMe"
  description = "Elastic Metal offer name (data.scaleway_baremetal_offer)."
}

variable "os_name" {
  type        = string
  default     = "Ubuntu"
  description = "Elastic Metal OS name (data.scaleway_baremetal_os)."
}

variable "os_version" {
  type        = string
  default     = "24.04 LTS (Noble Numbat)"
  description = "Elastic Metal OS version string, must match the Scaleway catalog exactly."
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

variable "postgres_admin_user" {
  type        = string
  description = "Admin user of the shared managed PG instance, used to create the Windmill roles (grants.tf)."
}

variable "postgres_admin_password" {
  type      = string
  sensitive = true
}

variable "postgres_password_windmill" {
  type      = string
  sensitive = true
}

variable "glitchtip_bucket_admin_user_ids" {
  type        = list(string)
  description = "IAM user IDs allowed to administer the GlitchTip bucket (everyone running OpenTofu on this target). Without them the bucket policy locks them out of the bucket."

  validation {
    condition     = length(var.glitchtip_bucket_admin_user_ids) > 0
    error_message = "At least one admin user ID is required, otherwise OpenTofu loses access to the bucket."
  }
}
