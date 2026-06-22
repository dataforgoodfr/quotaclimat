# S3 bucket for storing Mediatree video files.
resource "scaleway_object_bucket" "mediatree_videos" {
  name       = "mediatree-videos-${var.environment}"
  region     = "fr-par"
  project_id = scaleway_account_project.project.id
}

# Dedicated IAM application/API key scoped to only this bucket.
resource "scaleway_iam_application" "mediatree_videos_application" {
  name        = "app-advertising-${var.environment}-mediatree-videos"
  description = "Application with read/write access to the mediatree-videos bucket only"
}

# Bucket policy grants the application read/write access on this bucket
# only, instead of an IAM policy (which can only scope by project).
resource "scaleway_object_bucket_policy" "mediatree_videos_policy" {
  bucket = scaleway_object_bucket.mediatree_videos.name
  region = scaleway_object_bucket.mediatree_videos.region

  policy = jsonencode({
    Version = "2023-04-17"
    Id      = "mediatree-videos-read-write"
    Statement = [
      {
        Sid    = "AllowMediatreeVideosReadWrite"
        Effect = "Allow"
        Principal = {
          SCW = "application_id:${scaleway_iam_application.mediatree_videos_application.id}"
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          scaleway_object_bucket.mediatree_videos.name,
          "${scaleway_object_bucket.mediatree_videos.name}/*",
        ]
      }
    ]
  })
}

resource "scaleway_iam_api_key" "mediatree_videos_api_key" {
  application_id     = scaleway_iam_application.mediatree_videos_application.id
  description        = "API key restricted to the mediatree-videos bucket (read/write)"
  default_project_id = scaleway_account_project.project.id
}
