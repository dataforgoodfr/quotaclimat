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
