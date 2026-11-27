output "project_id" {
  value = scaleway_account_project.project.id
}

# label_studio_url of I18N_COUNTRY_SECRETS (see infrastructure/Readme.md)
output "labelstudio_urls" {
  value = { for country, container in scaleway_container.labelstudio_container : country => "https://${container.domain_name}" }
}

output "bucket_names" {
  value = { for country, bucket in scaleway_object_bucket.bucket : country => bucket.name }
}
