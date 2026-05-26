# Create a dedicated Scaleway project for this environment.
resource "scaleway_account_project" "project" {
  name = "rrs-${var.environment}"
}

# Scaleway sometimes returns 403 immediately after project creation before it has fully propagated.
resource "time_sleep" "wait_for_project" {
  create_duration = "10s"
  depends_on      = [scaleway_account_project.project]
}

# PostgreSQL instance.
resource "scaleway_rdb_instance" "rrs_rdb" {
  name                = "rdb-rrs-${var.environment}"
  node_type           = var.node_type
  volume_size_in_gb   = var.volume_size_in_gb
  volume_type         = var.volume_type
  engine              = "PostgreSQL-15"
  is_ha_cluster       = false
  disable_backup      = true
  project_id          = scaleway_account_project.project.id
  user_name           = "rrs-admin-${var.environment}"
  depends_on          = [time_sleep.wait_for_project]
  password_wo         = var.postgres_admin_password
  password_wo_version = var.postgres_admin_password_version
  region              = "fr-par"
}

# Create the rrs database on the instance.
resource "scaleway_rdb_database" "rrs" {
  instance_id = scaleway_rdb_instance.rrs_rdb.id
  name        = "rrs"
}

# Grant the admin user full privileges on the rrs database.
resource "scaleway_rdb_privilege" "rrs_admin" {
  instance_id   = scaleway_rdb_instance.rrs_rdb.id
  user_name     = "rrs-admin-${var.environment}"
  database_name = scaleway_rdb_database.rrs.name
  permission    = "all"
}

# Migration user — admin so it can run DDL (CREATE/ALTER TABLE via Alembic).
# Using a standalone scaleway_rdb_user keeps the password in sync with the secret,
# unlike the instance-level password_wo which is write-only and can silently diverge.
resource "scaleway_rdb_user" "rrs_migrate_user" {
  instance_id = scaleway_rdb_instance.rrs_rdb.id
  name        = "rrs-migrate-${var.environment}"
  password    = var.postgres_migrate_password
  is_admin    = true
}

resource "scaleway_rdb_privilege" "rrs_migrate_user" {
  instance_id   = scaleway_rdb_instance.rrs_rdb.id
  user_name     = scaleway_rdb_user.rrs_migrate_user.name
  database_name = scaleway_rdb_database.rrs.name
  permission    = "all"
}

# Job user with read_write access.
resource "scaleway_rdb_user" "rrs_job_user" {
  instance_id = scaleway_rdb_instance.rrs_rdb.id
  name        = "rrs-main-${var.environment}"
  password    = var.postgres_job_password
  is_admin    = false
}

resource "scaleway_rdb_privilege" "rrs_job_user" {
  instance_id   = scaleway_rdb_instance.rrs_rdb.id
  user_name     = scaleway_rdb_user.rrs_job_user.name
  database_name = scaleway_rdb_database.rrs.name
  permission    = "readwrite"
}

resource "scaleway_rdb_user" "rrs_metabase_user" {
  instance_id = scaleway_rdb_instance.rrs_rdb.id
  name        = "rrs-metabase-${var.environment}"
  password    = var.db_metabase_user_password
  is_admin    = false
}

resource "scaleway_rdb_privilege" "rrs_metabase_user" {
  instance_id   = scaleway_rdb_instance.rrs_rdb.id
  user_name     = scaleway_rdb_user.rrs_metabase_user.name
  database_name = scaleway_rdb_database.rrs.name
  permission    = "readonly"
}

locals {
  # IP ranges used by Scaleway serverless job workers.
  # Serverless jobs cannot attach to a VPC, so the DB must accept traffic from these CIDRs.
  scaleway_job_cidrs = [
    { ip = "62.210.0.0/16",    description = "Scaleway DC2" },
    { ip = "195.154.0.0/16",   description = "Scaleway DC3" },
    { ip = "212.129.0.0/18",   description = "Scaleway DC4" },
    { ip = "62.4.0.0/19",      description = "Scaleway DC5" },
    { ip = "212.83.128.0/19",  description = "Scaleway DC6" },
    { ip = "212.83.160.0/19",  description = "Scaleway DC7" },
    { ip = "212.47.224.0/19",  description = "Scaleway DC8" },
    { ip = "163.172.0.0/16",   description = "Scaleway DC9" },
    { ip = "51.15.0.0/16",     description = "Scaleway DC10" },
    { ip = "151.115.0.0/16",   description = "Scaleway DC11" },
    { ip = "51.158.0.0/15",    description = "Scaleway DC12" },
    { ip = "78.232.0.0/16",    description = "Scaleway DC13" },
  ]

  acl_rules = var.environment == "dev" ? concat(
    local.scaleway_job_cidrs,
    [{ ip = "0.0.0.0/0", description = "Allow all (dev only)" }]
  ) : concat(local.scaleway_job_cidrs, var.acl_allowed_ips)
}

resource "scaleway_rdb_acl" "public" {
  instance_id = scaleway_rdb_instance.rrs_rdb.id

  dynamic "acl_rules" {
    for_each = local.acl_rules
    content {
      ip          = acl_rules.value.ip
      description = acl_rules.value.description
    }
  }
}
