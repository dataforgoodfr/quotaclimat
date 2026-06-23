output "project_api_key_access_key" {
  value = scaleway_iam_api_key.project_api_key.access_key
}

output "project_api_key_secret_key" {
  value     = scaleway_iam_api_key.project_api_key.secret_key
  sensitive = true
}

