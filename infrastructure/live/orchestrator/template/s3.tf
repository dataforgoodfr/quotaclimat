# S3 bucket for GlitchTip files (event attachments, source maps, uploads).
resource "scaleway_object_bucket" "glitchtip" {
  name       = "quotaclimat-glitchtip"
  region     = "fr-par"
  project_id = scaleway_account_project.project.id
}

# Dedicated IAM application + API key used only by GlitchTip.
resource "scaleway_iam_application" "glitchtip_s3" {
  name        = "glitchtip-s3"
  description = "GlitchTip: read/write access to the glitchtip bucket only."
}

resource "scaleway_iam_api_key" "glitchtip_s3" {
  application_id     = scaleway_iam_application.glitchtip_s3.id
  description        = "GlitchTip S3 access key (scoped to the glitchtip bucket by bucket policy)."
  default_project_id = scaleway_account_project.project.id
}

# Scoping: the application has NO IAM policy (so no project-wide Object
# Storage permissions). Its only access comes from this bucket policy, which
# is limited to this one bucket.
resource "scaleway_object_bucket_policy" "glitchtip" {
  bucket = scaleway_object_bucket.glitchtip.name
  region = "fr-par"

  policy = jsonencode({
    Version = "2023-04-17"
    Id      = "glitchtip-app-access"
    Statement = [
      {
        Sid    = "GlitchtipListBucket"
        Effect = "Allow"
        Principal = {
          SCW = "application_id:${scaleway_iam_application.glitchtip_s3.id}"
        }
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads",
        ]
        Resource = [scaleway_object_bucket.glitchtip.name]
      },
      {
        Sid    = "GlitchtipObjectReadWrite"
        Effect = "Allow"
        Principal = {
          SCW = "application_id:${scaleway_iam_application.glitchtip_s3.id}"
        }
        # Multipart actions: DuckDB writes large Parquet files for cold storage.
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = ["${scaleway_object_bucket.glitchtip.name}/*"]
      },
    ]
  })
}
