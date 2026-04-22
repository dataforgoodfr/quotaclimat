variable "environment" {
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
