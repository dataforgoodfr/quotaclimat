output "kestra_url" {
  description = "Public URL of the Kestra serverless container"
  value       = scaleway_container.kestra.public_endpoint
}

output "kestra_db_name" {
  description = "Name of the Kestra PostgreSQL database"
  value       = scaleway_rdb_database.kestra_db.name
}

output "kestra_s3_bucket" {
  description = "Name of the S3 bucket used as Kestra storage backend"
  value       = scaleway_object_bucket.kestra_storage.name
}
