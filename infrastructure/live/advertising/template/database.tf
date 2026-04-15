data "scaleway_rdb_instance" "barometre_rdb" {
  name       = var.barometre_pg_instance_name
  project_id = data.scaleway_account_project.barometre.id
}

data "scaleway_rdb_database" "barometre" {
  instance_id = data.scaleway_rdb_instance.barometre_rdb.id
  name        = "barometre"
}

# Create PostgreSQL user
resource "scaleway_rdb_user" "labelstudio_user" {
  instance_id = data.scaleway_rdb_instance.barometre_rdb.id
  name        = "labelstudio-advertising-${var.environment}"
  password    = var.postgres_password_labelstudio
}

# Create PostgreSQL database
resource "scaleway_rdb_database" "labelstudio_db" {
  instance_id = data.scaleway_rdb_instance.barometre_rdb.id
  name        = "labelstudio-advertising-${var.environment}-db"
}

resource "scaleway_rdb_privilege" "labelstudio_policy" {
  instance_id   = data.scaleway_rdb_instance.barometre_rdb.id
  user_name     = scaleway_rdb_user.labelstudio_user.name
  database_name = scaleway_rdb_database.labelstudio_db.name
  permission    = "all"

  depends_on = [scaleway_rdb_user.labelstudio_user, scaleway_rdb_database.labelstudio_db]
}
