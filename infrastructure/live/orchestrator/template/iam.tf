data "scaleway_account_project" "barometre" {
  name = var.barometre_project_name
}

data "scaleway_account_project" "policy_projects" {
  for_each = toset(var.policy_project_names)
  name     = each.key
}

resource "scaleway_iam_application" "orchestrator_app" {
  name        = "app-kestra-${var.environment}"
  description = "Application handling rights for the Kestra orchestrator in ${var.environment} environment"
}

resource "scaleway_iam_policy" "orchestrator_policy" {
  name           = "app-kestra-${var.environment}-policy"
  application_id = scaleway_iam_application.orchestrator_app.id

  rule {
    project_ids     = var.environment != "prod" ? [for p in data.scaleway_account_project.policy_projects : p.id] : null
    organization_id = var.environment == "prod" ? data.scaleway_account_project.barometre.organization_id : null
    permission_set_names = [
      "ObjectStorageObjectsRead",
      "ObjectStorageObjectsWrite",
      "ObjectStorageBucketsRead",
      "ObjectStorageBucketsWrite",
      "ServerlessJobsFullAccess",
    ]
  }
}

resource "scaleway_iam_api_key" "orchestrator_api_key" {
  application_id     = scaleway_iam_application.orchestrator_app.id
  description        = "API key for app-kestra-${var.environment}"
  default_project_id = data.scaleway_account_project.barometre.id
}
