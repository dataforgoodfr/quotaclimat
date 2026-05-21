output "registry_endpoint" {
  value       = scaleway_registry_namespace.rrs.endpoint
  description = "Container registry endpoint — tag and push images here."
}

output "rdb_endpoint_ip" {
  value       = scaleway_rdb_instance.rrs_rdb.endpoint_ip
  description = "Public endpoint IP of the RDB instance."
}

output "rdb_endpoint_port" {
  value       = scaleway_rdb_instance.rrs_rdb.endpoint_port
  description = "Public endpoint port of the RDB instance."
}

output "job_clustering_id" {
  value = scaleway_serverless_job.rrs_clustering.id
}

output "job_import_segments_id" {
  value = scaleway_serverless_job.rrs_import_segments.id
}

output "job_import_claims_id" {
  value = scaleway_serverless_job.rrs_import_cases.id
}

output "ci_access_key" {
  value       = scaleway_iam_api_key.rrs_ci.access_key
  description = "Access key for the rrs-ci application — set as CI secret SCW_ACCESS_KEY."
}

output "ci_secret_key" {
  value       = scaleway_iam_api_key.rrs_ci.secret_key
  sensitive   = true
  description = "Secret key for the rrs-ci application — set as CI secret SCW_SECRET_KEY."
}
