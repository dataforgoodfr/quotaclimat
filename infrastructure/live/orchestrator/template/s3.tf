# S3 bucket for GlitchTip files (event attachments, source maps, uploads).
resource "scaleway_object_bucket" "glitchtip" {
  name       = "quotaclimat-glitchtip-v2"
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

# Scoping: the application's IAM policy (below) is limited to this project and
# this bucket policy limits it to this one bucket.
# Once a bucket policy exists, Scaleway denies every principal it does not name,
# including the people running OpenTofu (GetBucketCors, ... then fail with 403).
# The first statement therefore names them (var.glitchtip_bucket_admin_user_ids).
resource "scaleway_object_bucket_policy" "glitchtip" {
  bucket = scaleway_object_bucket.glitchtip.name
  region = "fr-par"
  # Without this the policy is sent with the provider's default project, not the
  # bucket's, and Scaleway answers 403 AccessDenied.
  project_id = scaleway_account_project.project.id

  policy = jsonencode({
    Version = "2023-04-17"
    Id      = "glitchtip-app-access"
    Statement = [
      {
        Sid    = "BucketAdmins"
        Effect = "Allow"
        Principal = {
          SCW = [for id in var.glitchtip_bucket_admin_user_ids : "user_id:${id}"]
        }
        Action = ["s3:*"]
        Resource = [
          scaleway_object_bucket.glitchtip.name,
          "${scaleway_object_bucket.glitchtip.name}/*",
        ]
      },
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

# Scaleway evaluates the application's IAM permissions together with the bucket
# policy, so an application with no IAM permission is denied even when the
# bucket policy allows it. Scoped to this project, which only holds the
# GlitchTip bucket (the Terraform state bucket lives in another project).
resource "scaleway_iam_policy" "glitchtip_s3" {
  name           = "glitchtip-s3-policy"
  application_id = scaleway_iam_application.glitchtip_s3.id


  rule {
    project_ids = [scaleway_account_project.project.id]
    permission_set_names = [
      "ObjectStorageObjectsRead",
      "ObjectStorageObjectsWrite",
      "ObjectStorageObjectsDelete",
      "ObjectStorageBucketsRead",
    ]
  }
}
