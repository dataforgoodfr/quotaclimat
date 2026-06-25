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

