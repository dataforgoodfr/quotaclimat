resource "scaleway_iam_application" "rrs_ci" {
  name        = "rrs-ci-${var.environment}"
  description = "CI application for pushing images to the rrs ${var.environment} container registry."
}

resource "scaleway_iam_policy" "rrs_ci" {
  name           = "rrs-ci-${var.environment}-policy"
  application_id = scaleway_iam_application.rrs_ci.id

  rule {
    project_ids = [scaleway_account_project.project.id]
    permission_set_names = [
      "ContainerRegistryFullAccess",
      "ServerlessJobsFullAccess",
      "ObjectStorageObjectsRead",
      "ObjectStorageObjectsWrite",
      "ObjectStorageBucketsRead",
      "ObjectStorageBucketsWrite",
    ]
  }
}

resource "scaleway_iam_api_key" "rrs_ci" {
  application_id     = scaleway_iam_application.rrs_ci.id
  description        = "API key for rrs-ci-${var.environment} — used by CI to push images."
  default_project_id = scaleway_account_project.project.id
}

# Grants the two requested IAM groups read/write access to Object Storage in
# this project. Scaleway bucket policies (scaleway_object_bucket_policy) only
# accept user_id/application_id principals, not group_id — so group access
# can only be granted at the project level via an IAM policy, not scoped to
# the expert_model_source_documents bucket alone.
resource "scaleway_iam_policy" "expert_model_source_documents_group_access" {
  for_each = toset([
    "c1e81bd0-f618-48d6-a77c-ada4f2c0e1bb",
    "eac98da3-f17b-4ac2-9762-3681adfa6f38",
  ])

  name     = "emsd-${var.environment}-group-${substr(each.value, 0, 8)}-policy"
  group_id = each.value

  rule {
    project_ids = [scaleway_account_project.project.id]
    permission_set_names = [
      "ObjectStorageObjectsRead",
      "ObjectStorageObjectsWrite",
      "ObjectStorageBucketsRead",
      "ObjectStorageBucketsWrite",
    ]
  }
}
