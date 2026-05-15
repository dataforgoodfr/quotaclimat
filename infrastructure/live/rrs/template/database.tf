# Create a dedicated Scaleway project for this environment.
resource "scaleway_account_project" "project" {
  name = "rss-${var.environment}"
}

# PostgreSQL instance.
resource "scaleway_rdb_instance" "rss_rdb" {
  name                = "rdb-rss-${var.environment}"
  node_type           = var.node_type
  volume_size_in_gb   = var.volume_size_in_gb
  volume_type         = var.volume_type
  engine              = "PostgreSQL-15"
  is_ha_cluster       = false
  disable_backup      = true
  project_id          = scaleway_account_project.project.id
  user_name           = "rss-admin-${var.environment}"
  password_wo         = var.postgres_admin_password
  password_wo_version = var.postgres_admin_password_version
  region              = "fr-par"
}

# Create the rss database on the instance.
resource "scaleway_rdb_database" "rss" {
  instance_id = scaleway_rdb_instance.rss_rdb.id
  name        = "rss"
}

# Grant the admin user full privileges on the rss database.
resource "scaleway_rdb_privilege" "rss_admin" {
  instance_id   = scaleway_rdb_instance.rss_rdb.id
  user_name     = "rss-admin-${var.environment}"
  database_name = scaleway_rdb_database.rss.name
  permission    = "all"
}

# Job user with read_write access.
resource "scaleway_rdb_user" "rss_job_user" {
  instance_id = scaleway_rdb_instance.rss_rdb.id
  name        = "rss-main-${var.environment}"
  password    = var.postgres_job_password
  is_admin    = false
}

resource "scaleway_rdb_privilege" "rss_job_user" {
  instance_id   = scaleway_rdb_instance.rss_rdb.id
  user_name     = scaleway_rdb_user.rss_job_user.name
  database_name = scaleway_rdb_database.rss.name
  permission    = "readwrite"
}

# In dev, allow all traffic. In other environments, restrict to provided CIDRs.
resource "scaleway_rdb_acl" "public" {
  instance_id = scaleway_rdb_instance.rss_rdb.id

  dynamic "acl_rules" {
    for_each = var.environment == "dev" ? [{ ip = "0.0.0.0/0", description = "Allow all (dev only)" }] : var.acl_allowed_ips
    content {
      ip          = acl_rules.value.ip
      description = acl_rules.value.description
    }
  }
}
