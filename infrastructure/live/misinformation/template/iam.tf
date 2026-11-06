# One IAM application per country: used by the Label Studio container and the
# job to read/write the country bucket and the barometre project.
resource "scaleway_iam_application" "project_application" {
  for_each = var.countries

  name        = "app-${var.subject}-${each.key}-${var.environment}"
  description = "Application handling the permissions for the label misinformation for project ${scaleway_account_project.project.name}"
}

resource "scaleway_iam_policy" "project_application" {
  for_each = var.countries

  name           = "app-${var.subject}-${each.key}-${var.environment}-policy"
  application_id = scaleway_iam_application.project_application[each.key].id

  rule {
    project_ids = [
      scaleway_account_project.project.id,
      data.scaleway_account_project.barometre.id,
    ]
    permission_set_names = [
      "ContainersFullAccess",
      "ContainerRegistryReadOnly",
      "ObjectStorageObjectsRead",
      "ObjectStorageObjectsWrite",
      "ObjectStorageBucketsRead",
      "ObjectStorageBucketsWrite",
      "PrivateNetworksReadOnly",
      "RelationalDatabasesReadOnly",
      "SecretManagerReadOnly",
      "ServerlessJobsFullAccess",
    ]
  }
}

resource "scaleway_iam_api_key" "project_api_key" {
  for_each = var.countries

  application_id     = scaleway_iam_application.project_application[each.key].id
  description        = "API key for the app-${var.subject}-${each.key}-${var.environment} application"
  default_project_id = scaleway_account_project.project.id
}
