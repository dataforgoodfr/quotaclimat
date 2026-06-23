output "project_id" {
  value = scaleway_account_project.project.id
}

output "instance_id" {
  value = scaleway_rdb_instance.barometre_rdb.id
}

output "instance_endpoint_ip" {
  value = scaleway_rdb_instance.barometre_rdb.endpoint_ip
}

output "instance_endpoint_port" {
  value = scaleway_rdb_instance.barometre_rdb.endpoint_port
}

output "database_name" {
  value = scaleway_rdb_database.barometre.name
}

output "rrs_read_user" {
  value = scaleway_rdb_user.rrs_read.name
}
output "mediatree_videos_api_key_access_key" {
  value = scaleway_iam_api_key.mediatree_videos_api_key.access_key
}

output "mediatree_videos_api_key_secret_key" {
  value     = scaleway_iam_api_key.mediatree_videos_api_key.secret_key
  sensitive = true
}
