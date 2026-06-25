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
