variable "environment" {
  type = string
}


variable "postgres_admin_password" {
  type      = string
  sensitive = true
}

