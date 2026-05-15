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
