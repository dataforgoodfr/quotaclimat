# S3 bucket for storing Mediatree video files.
resource "scaleway_object_bucket" "mediatree_videos" {
  name       = "mediatree-videos-${var.environment}"
  region     = "fr-par"
  project_id = scaleway_account_project.project.id
}

# Dedicated IAM application/API key scoped to the advertising project.
# Scaleway IAM policies can only scope by project (not by bucket), so
# this key technically covers any bucket in the project, not just
# mediatree-videos — there is only one bucket in this project today.
resource "scaleway_iam_application" "mediatree_videos_application" {
  name        = "app-${var.environment}-mediatree-videos"
  description = "Application with read/write access to the mediatree-videos bucket"
}

resource "scaleway_iam_policy" "mediatree_videos_policy" {
  name           = "app-${var.environment}-mediatree-videos-policy"
  application_id = scaleway_iam_application.mediatree_videos_application.id

  rule {
    project_ids = [
      scaleway_account_project.project.id,
    ]
    permission_set_names = [
      "ObjectStorageObjectsRead",
      "ObjectStorageObjectsWrite",
      "ObjectStorageBucketsRead",
      "ObjectStorageBucketsWrite",
    ]
  }
}

resource "scaleway_iam_api_key" "mediatree_videos_api_key" {
  application_id     = scaleway_iam_application.mediatree_videos_application.id
  description        = "API key for app-${var.environment}-mediatree-videos"
  default_project_id = scaleway_account_project.project.id
}
