output "instance_ip" {
  value = scaleway_baremetal_server.orchestrator.ipv4[0].address
}

output "postgres_host" {
  value = data.scaleway_rdb_instance.shared.endpoint_ip
}

output "postgres_port" {
  value = data.scaleway_rdb_instance.shared.endpoint_port
}

output "kestra_db_name" {
  value = scaleway_rdb_database.kestra.name
}

output "kestra_db_user" {
  value = scaleway_rdb_user.kestra.name
}

output "glitchtip_db_name" {
  value = scaleway_rdb_database.glitchtip.name
}

output "glitchtip_db_user" {
  value = scaleway_rdb_user.glitchtip.name
}

output "glitchtip_s3_bucket" {
  value = scaleway_object_bucket.glitchtip.name
}

output "glitchtip_s3_access_key" {
  value = scaleway_iam_api_key.glitchtip_s3.access_key
}

output "glitchtip_s3_secret_key" {
  value     = scaleway_iam_api_key.glitchtip_s3.secret_key
  sensitive = true
}

output "windmill_db_name" {
  value = scaleway_rdb_database.windmill.name
}

output "windmill_db_user" {
  value = scaleway_rdb_user.windmill.name
}
