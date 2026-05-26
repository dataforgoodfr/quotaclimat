resource "scaleway_iam_application" "rrs_ci" {
  name        = "rrs-ci-${var.environment}"
  description = "CI application for pushing images to the rrs ${var.environment} container registry."
}

resource "scaleway_iam_policy" "rrs_ci" {
  name           = "rrs-ci-${var.environment}-policy"
  application_id = scaleway_iam_application.rrs_ci.id

  rule {
    project_ids          = [scaleway_account_project.project.id]
    permission_set_names = [
      "ContainerRegistryFullAccess",
      "ServerlessJobsFullAccess"
    ]
  }
}

resource "scaleway_iam_api_key" "rrs_ci" {
  application_id     = scaleway_iam_application.rrs_ci.id
  description        = "API key for rrs-ci-${var.environment} — used by CI to push images."
  default_project_id = scaleway_account_project.project.id
}
