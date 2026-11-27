# Scaleway Secret Manager entries. Kestra's barometre-i18n flow reads the
# per-country ones (I18N_COUNTRY_SECRETS, see infrastructure/Readme.md): keep
# their names and paths.

## Shared secrets (path /common), one per environment
######################################################################

# MEDIATREE_PASSWORD
resource "scaleway_secret" "mediatree_password" {
  name       = "mediatree-password"
  path       = "/common"
  project_id = scaleway_account_project.project.id
}
resource "scaleway_secret_version" "mediatree_password" {
  secret_id = scaleway_secret.mediatree_password.id
  data      = var.mediatree_password
}

# OPENAI_API_KEY
resource "scaleway_secret" "openai_api_key" {
  name       = "openai-api-key"
  path       = "/common"
  project_id = scaleway_account_project.project.id
}
resource "scaleway_secret_version" "openai_api_key" {
  secret_id = scaleway_secret.openai_api_key.id
  data      = var.openai_api_key
}

# MISTRAL_API_KEY
resource "scaleway_secret" "mistral_api_key" {
  name       = "mistral-api-key"
  path       = "/common"
  project_id = scaleway_account_project.project.id
}
resource "scaleway_secret_version" "mistral_api_key" {
  secret_id = scaleway_secret.mistral_api_key.id
  data      = var.mistral_api_key
}

# POSTGRES_PASSWORD (barometre database)
resource "scaleway_secret" "postgres_password_barometre" {
  name       = "postgres_password-barometre"
  path       = "/common"
  project_id = scaleway_account_project.project.id
}
resource "scaleway_secret_version" "postgres_password_barometre" {
  secret_id = scaleway_secret.postgres_password_barometre.id
  data      = var.postgres_password_barometre
}

## Per-country secrets (path /<country>)
######################################################################

# BUCKET_ACCESS
resource "scaleway_secret" "access_key_id" {
  for_each = var.countries

  name       = "access-key-${each.key}"
  path       = "/${each.key}"
  project_id = scaleway_account_project.project.id
}
resource "scaleway_secret_version" "access_key_id" {
  for_each = var.countries

  secret_id = scaleway_secret.access_key_id[each.key].id
  data      = scaleway_iam_api_key.project_api_key[each.key].access_key
}

# BUCKET_SECRET
resource "scaleway_secret" "secret_key_id" {
  for_each = var.countries

  name       = "secret-key-${each.key}"
  path       = "/${each.key}"
  project_id = scaleway_account_project.project.id
}
resource "scaleway_secret_version" "secret_key_id" {
  for_each = var.countries

  secret_id = scaleway_secret.secret_key_id[each.key].id
  data      = scaleway_iam_api_key.project_api_key[each.key].secret_key
}

# API_LABEL_STUDIO_KEY (Label Studio user token)
resource "scaleway_secret" "secret_label_studio_token" {
  for_each = var.countries

  name       = "labelstudio-${each.key}-token"
  path       = "/${each.key}"
  project_id = scaleway_account_project.project.id
}
resource "scaleway_secret_version" "api_label_studio_key_value" {
  for_each = var.countries

  secret_id = scaleway_secret.secret_label_studio_token[each.key].id
  data      = var.country_secrets[each.key].labelstudio_user_token
}
