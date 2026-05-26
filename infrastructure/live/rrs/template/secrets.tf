resource "scaleway_secret" "postgres_admin_password" {
  name       = "rrs-admin-password-${var.environment}"
  project_id = scaleway_account_project.project.id
  region     = "fr-par"
}

resource "scaleway_secret_version" "postgres_admin_password" {
  secret_id = scaleway_secret.postgres_admin_password.id
  data      = var.postgres_admin_password
}

resource "scaleway_secret" "rrs_job_password" {
  name       = "rrs-job-password-${var.environment}"
  project_id = scaleway_account_project.project.id
  region     = "fr-par"
}

resource "scaleway_secret_version" "rrs_job_password" {
  secret_id = scaleway_secret.rrs_job_password.id
  data      = var.postgres_job_password
}

resource "scaleway_secret" "mistral_api_key" {
  name       = "rrs-mistral-api-key-${var.environment}"
  project_id = scaleway_account_project.project.id
  region     = "fr-par"
}

resource "scaleway_secret_version" "mistral_api_key" {
  secret_id = scaleway_secret.mistral_api_key.id
  data      = var.mistral_api_key
}

resource "scaleway_secret" "anthropic_api_key" {
  name       = "rrs-anthropic-api-key-${var.environment}"
  project_id = scaleway_account_project.project.id
  region     = "fr-par"
}

resource "scaleway_secret_version" "anthropic_api_key" {
  secret_id = scaleway_secret.anthropic_api_key.id
  data      = var.anthropic_api_key
}

resource "scaleway_secret" "barometre_rrs_read_password" {
  name       = "rrs-barometre-read-password-${var.environment}"
  project_id = scaleway_account_project.project.id
  region     = "fr-par"
}

resource "scaleway_secret_version" "barometre_rrs_read_password" {
  secret_id = scaleway_secret.barometre_rrs_read_password.id
  data      = var.barometre_rrs_read_password
}

