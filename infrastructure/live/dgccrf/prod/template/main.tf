data "scaleway_rdb_instance" "barometre_rdb" {
  name       = var.barometre_pg_instance_name
  project_id = data.scaleway_account_project.barometre.id
}

data "scaleway_rdb_database" "barometre" {
  instance_id = data.scaleway_rdb_instance.barometre_rdb.id
  name        = "barometre"
}

resource "scaleway_rdb_user" "dgccrf_user" {
  instance_id = scaleway_rdb_instance.my_instance.id
  name        = "dgccrf-prod"
  password    = var.dgccrf_user_password
  is_admin    = false
}

resource "scaleway_rdb_privilege" "dgccrf_user_policy" {
  instance_id   = data.scaleway_rdb_instance.barometre_rdb.id
  user_name     = scaleway_rdb_user.dgccrf_user.name
  database_name = data.scaleway_rdb_database.barometre.name
  permission    = "readonly"
  depends_on    = [scaleway_rdb_user.dgccrf_user]
}

resource "null_resource" "table_grants" {
  triggers = {
    user_name   = scaleway_rdb_user.my_user.name
    instance_id = scaleway_rdb_instance.my_instance.id
    sql_hash    = filemd5("${path.module}/grants.sql")
  }

  depends_on = [scaleway_rdb_privilege.my_privilege]

  provisioner "local-exec" {
    command = <<-EOF
      docker run --rm \
        -e PGPASSWORD=${var.postgres_admin_password} \
        -v ${path.module}/grants.sql:/grants.sql \
        postgres:15 \
        psql \
          -h ${data.scaleway_rdb_instance.barometre_rdb.load_balancer[0].ip} \
          -p ${data.scaleway_rdb_instance.barometre_rdb.load_balancer[0].port} \
          -U ${var.postgres_admin_user} \
          -d ${var.postgres_db_name} \
          -v db_user=${scaleway_rdb_user.dgccrf_user.name} \
          -f /grants.sql
    EOF
  }
}