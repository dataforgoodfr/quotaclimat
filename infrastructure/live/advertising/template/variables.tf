variable "environment" {
  type = string
}
variable "labelstudio_admin_password" {
  type      = string
  sensitive = true
}

variable "labelstudio_user_token" {
  type      = string
  sensitive = true
}

variable "postgres_password_labelstudio" {
  type      = string
  sensitive = true
}

variable "barometre_pg_instance_name" {
  type    = string
  default = "rdb-poc"
}