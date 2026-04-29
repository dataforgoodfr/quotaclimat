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

variable "barometre_project_name" {
  type    = string
  default = "barometre-dev"
}

variable "barometre_pg_instance_name" {
  type    = string
  default = "rdb-barometre-dev"
}

variable "dgccrf_user_password" {
  type      = string
  sensitive = true
}

variable "postgres_host" {
  type = string
}

variable "postgres_port" {
  type    = number
  default = 5432
}

variable "postgres_admin_user" {
  type      = string
  sensitive = true
}

variable "postgres_admin_password" {
  type      = string
  sensitive = true
}