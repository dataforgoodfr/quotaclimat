variable "environment" {
  type = string
}

variable "rdb_instance_name" {
  type = string
}

variable "postgres_admin_user" {
  type      = string
  sensitive = true
}

variable "postgres_admin_password" {
  type      = string
  sensitive = true
}
variable "barometre_rrs_read_password" {
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
  type = list(object({
    ip          = string
    description = string
  }))
  default     = []
  description = "List of CIDRs allowed to reach the database. Ignored in dev (0.0.0.0/0 is used instead)."
}

