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
  instance_id = scaleway_rdb_instance.barometre_rdb.id

  dynamic "acl_rules" {
    for_each = local.acl_rules
    content {
      ip          = acl_rules.value.ip
      description = acl_rules.value.description
    }
  }
}
