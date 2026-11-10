# Label Studio databases live on the barometre RDB instance.
data "scaleway_rdb_instance" "barometre_rdb" {
  name       = var.barometre_pg_instance_name
  project_id = data.scaleway_account_project.barometre.id
}

data "scaleway_rdb_database" "barometre" {
  instance_id = data.scaleway_rdb_instance.barometre_rdb.id
  name        = "barometre"
}

resource "scaleway_rdb_user" "labelstudio_user" {
  for_each = var.countries

  instance_id = data.scaleway_rdb_instance.barometre_rdb.id
  name        = "labelstudio-${var.subject}-${each.key}-${var.environment}"
  password    = var.country_secrets[each.key].postgres_password_labelstudio
}

resource "scaleway_rdb_database" "labelstudio_db" {
  for_each = var.countries

  instance_id = data.scaleway_rdb_instance.barometre_rdb.id
  name        = "labelstudio-${var.subject}-${each.key}-${var.environment}-db"
}

resource "scaleway_rdb_privilege" "labelstudio_policy" {
  for_each = var.countries

  instance_id   = data.scaleway_rdb_instance.barometre_rdb.id
  user_name     = scaleway_rdb_user.labelstudio_user[each.key].name
  database_name = scaleway_rdb_database.labelstudio_db[each.key].name
  permission    = "all"

  depends_on = [scaleway_rdb_user.labelstudio_user, scaleway_rdb_database.labelstudio_db]
}
