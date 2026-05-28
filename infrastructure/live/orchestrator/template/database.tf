data "scaleway_rdb_instance" "barometre_rdb" {
  name       = var.barometre_pg_instance_name
  project_id = data.scaleway_account_project.barometre.id
}

resource "scaleway_rdb_database" "kestra_db" {
  instance_id = data.scaleway_rdb_instance.barometre_rdb.id
  name        = "kestra-${var.environment}"
}

resource "scaleway_rdb_user" "kestra_user" {
  instance_id = data.scaleway_rdb_instance.barometre_rdb.id
  name        = "kestra-${var.environment}"
  password    = var.kestra_db_password
}

resource "scaleway_rdb_privilege" "kestra_privilege" {
  instance_id   = data.scaleway_rdb_instance.barometre_rdb.id
  user_name     = scaleway_rdb_user.kestra_user.name
  database_name = scaleway_rdb_database.kestra_db.name
  permission    = "all"

  depends_on = [scaleway_rdb_user.kestra_user, scaleway_rdb_database.kestra_db]
}
