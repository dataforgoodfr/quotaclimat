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

resource "scaleway_secret" "bucket" {
  name       = "rrs-bucket-access-key-${var.environment}"
  project_id = scaleway_account_project.project.id
  region     = "fr-par"
}

resource "scaleway_secret_version" "bucket" {
  secret_id = scaleway_secret.bucket.id
  data      = var.bucket
}

resource "scaleway_secret" "bucket_secret" {
  name       = "rrs-bucket-secret-key-${var.environment}"
  project_id = scaleway_account_project.project.id
  region     = "fr-par"
}

resource "scaleway_secret_version" "bucket_secret" {
  secret_id = scaleway_secret.bucket_secret.id
  data      = var.bucket_secret
}
