# Create a dedicated project for this environment.
resource "scaleway_account_project" "project" {
  name = "barometre-${var.environment}"
}

# PostgreSQL instance — smallest available node type for dev.
resource "scaleway_rdb_instance" "barometre_rdb" {
  name           = "rdb-barometre-${var.environment}"
  node_type      = "DB-DEV-S"
  engine         = "PostgreSQL-15"
  is_ha_cluster  = false
  disable_backup = true
  project_id     = scaleway_account_project.project.id
  user_name      = var.postgres_admin_user
  password       = var.postgres_admin_password
  region         = "fr-par"
}

# Create the barometre database on the new instance.
resource "scaleway_rdb_database" "barometre" {
  instance_id = scaleway_rdb_instance.barometre_rdb.id
  name        = "barometre"
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
