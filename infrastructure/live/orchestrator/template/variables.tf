variable "environment" {
  type = string
}

variable "barometre_project_name" {
  type    = string
  default = "barometre-dev"
}

variable "barometre_pg_instance_name" {
  type    = string
  default = "rdb-poc"
}

variable "kestra_db_password" {
  type      = string
  sensitive = true
}

variable "kestra_admin_email" {
  type    = string
  default = "admin@kestra.io"
}

variable "kestra_admin_password" {
  type      = string
  sensitive = true
}

variable "policy_project_names" {
  type        = list(string)
  description = "Projects the Kestra IAM application gets access to. Empty list grants organisation-wide access."
  default     = []
}

variable "kestra_image_tag" {
  type        = string
  description = "Tag of the Kestra image on Docker Hub (e.g. v1.2)."
  default     = "v1.2"
}
