# Create a dedicated Scaleway project for this environment.
resource "scaleway_account_project" "project" {
  name = "advertising-${var.environment}"
}

# Look up the pre-existing barometre project.
data "scaleway_account_project" "barometre" {
  name = "barometre"
}

# IAM application that the Label Studio container authenticates as.
resource "scaleway_iam_application" "project_application" {
  name        = "app-advertising-${var.environment}"
  description = "Application handling S3 access for the advertising ${var.environment} environment"
}

# Grant the application S3 read access on both the environment project and barometre.
resource "scaleway_iam_policy" "project_policy" {
  name           = "app-advertising-${var.environment}-policy"
  application_id = scaleway_iam_application.project_application.id

  rule {
    project_ids = [
      scaleway_account_project.project.id,
      data.scaleway_account_project.barometre.id,
    ]
    permission_set_names = [
      "ObjectStorageObjectsRead",
      "ObjectStorageObjectsWrite",
      "ObjectStorageBucketsRead",
      "ObjectStorageBucketsWrite",
    ]
  }
}

# API key used by the container to authenticate as the application.
resource "scaleway_iam_api_key" "project_api_key" {
  application_id     = scaleway_iam_application.project_application.id
  description        = "API key for app-advertising-${var.environment}"
  default_project_id = scaleway_account_project.project.id
}
