variable "postgres_admin_password" {
  type      = string
  sensitive = true
}
variable "postgres_admin_user" {
  type      = string
  sensitive = true
}
variable "dgccrf_user_password" {
  type      = string
  sensitive = true
}
variable "postgres_db_name" {
  type    = string
  default = "barometre"
}
variable "barometre_pg_instance_name" {
  type    = string
  default = "rdb-poc"
}