provider "postgresql" {
  host      = var.postgres_host
  port      = var.postgres_port
  username  = var.postgres_admin_user
  password  = var.postgres_admin_password
  database  = data.scaleway_rdb_database.barometre.name
  sslmode   = "require"
  superuser = false
}

resource "postgresql_grant" "dgccrf_schema_usage" {
  database    = data.scaleway_rdb_database.barometre.name
  role        = scaleway_rdb_user.dgccrf_user.name
  schema      = "advertising"
  object_type = "schema"
  privileges  = ["USAGE"]
  depends_on  = [scaleway_rdb_user.dgccrf_user]
}

resource "postgresql_grant" "dgccrf_table_write" {
  database    = data.scaleway_rdb_database.barometre.name
  role        = scaleway_rdb_user.dgccrf_user.name
  schema      = "advertising"
  object_type = "table"
  objects     = ["ad", "ad_occurences"]
  privileges  = ["INSERT", "UPDATE", "DELETE"]
  depends_on  = [scaleway_rdb_user.dgccrf_user]
}
