# Create a dedicated project for this environment.
resource "scaleway_account_project" "project" {
  name = "barometre-${var.environment}"
}

# PostgreSQL instance — smallest available node type for dev.
resource "scaleway_rdb_instance" "barometre_rdb" {
  name                = "rdb-barometre-${var.environment}"
  node_type           = var.node_type
  volume_size_in_gb   = var.volume_size_in_gb
  volume_type         = var.volume_type
  engine              = "PostgreSQL-15"
  is_ha_cluster       = false
  disable_backup      = true
  project_id          = scaleway_account_project.project.id
  user_name           = var.postgres_admin_user
  password_wo         = var.postgres_admin_password
  password_wo_version = var.postgres_admin_password_version
  region              = "fr-par"
}

# Create the barometre database on the new instance.
resource "scaleway_rdb_database" "barometre" {
  instance_id = scaleway_rdb_instance.barometre_rdb.id
  name        = "barometre"
}

# Grant the admin user full privileges on the barometre database.
resource "scaleway_rdb_privilege" "barometre_admin" {
  instance_id   = scaleway_rdb_instance.barometre_rdb.id
  user_name     = var.postgres_admin_user
  database_name = scaleway_rdb_database.barometre.name
  permission    = "all"
}

resource "scaleway_rdb_user" "rrs_read" {
  instance_id = scaleway_rdb_instance.barometre_rdb.id
  name        = "rrs-read-${var.environment}"
  password    = var.barometre_rrs_read_password
  is_admin    = false
}

resource "scaleway_rdb_privilege" "rrs_read" {
  instance_id   = scaleway_rdb_instance.barometre_rdb.id
  user_name     = scaleway_rdb_user.rrs_read.name
  database_name = scaleway_rdb_database.barometre.name
  permission    = "readonly"
}

# Allow public access so the migration script can reach the instance.
# Dev only — restrict to specific IPs in production.
resource "scaleway_rdb_acl" "public" {
  instance_id = scaleway_rdb_instance.barometre_rdb.id
  acl_rules {
    ip          = "0.0.0.0/0"
    description = "Allow all (dev only)"
  }
}
