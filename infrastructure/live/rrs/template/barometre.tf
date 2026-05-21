data "scaleway_account_project" "barometre" {
  name = "barometre"
}

data "scaleway_rdb_instance" "barometre" {
  name       = "rdb-poc"
  project_id = data.scaleway_account_project.barometre.id
}

data "scaleway_rdb_database" "barometre" {
  instance_id = data.scaleway_rdb_instance.barometre.id
  name        = "barometre"
}

resource "scaleway_rdb_user" "rrs_read" {
  instance_id = data.scaleway_rdb_instance.barometre.id
  name        = "rrs-read-${var.environment}"
  password    = var.barometre_rrs_read_password
  is_admin    = false
}

resource "scaleway_rdb_privilege" "rrs_read" {
  instance_id   = data.scaleway_rdb_instance.barometre.id
  user_name     = scaleway_rdb_user.rrs_read.name
  database_name = data.scaleway_rdb_database.barometre.name
  permission    = "readonly"
}
