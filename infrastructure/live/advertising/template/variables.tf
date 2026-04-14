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