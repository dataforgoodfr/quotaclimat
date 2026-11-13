# --- Container Registry cleanup ---
# Scaleway Container Registry has no retention policy: every push on main adds a
# version tag (`poetry version patch`) and nothing deletes the old ones. This job
# deletes, once a week, every tag of the namespace except the most recent ones,
# `latest` and the tags still referenced by a job or a container
# (scripts/registry_cleanup.py). Enabled per environment with
# `registry_cleanup_enabled` (prod only: the dev namespace has its own
# hand-made `job-empty-registry` job).

locals {
  registry_cleanup = var.registry_cleanup_enabled ? toset(["enabled"]) : toset([])
}

data "scaleway_registry_namespace" "cleanup" {
  for_each   = local.registry_cleanup
  name       = var.registry_namespace_name
  region     = "fr-par"
  project_id = scaleway_account_project.project.id
}

resource "scaleway_iam_application" "registry_cleanup" {
  for_each    = local.registry_cleanup
  name        = "app-${var.environment}-registry-cleanup"
  description = "Deletes old tags of the ${var.registry_namespace_name} registry namespace (registry-cleanup-${var.environment} job)"
}

resource "scaleway_iam_policy" "registry_cleanup" {
  for_each       = local.registry_cleanup
  name           = "app-${var.environment}-registry-cleanup-policy"
  application_id = scaleway_iam_application.registry_cleanup[each.key].id

  # Delete tags of the namespace's project.
  rule {
    project_ids          = [scaleway_account_project.project.id]
    permission_set_names = ["ContainerRegistryFullAccess"]
  }

  # Read the jobs and containers of every project, to keep the tags they use.
  rule {
    organization_id      = scaleway_account_project.project.organization_id
    permission_set_names = ["ServerlessJobsReadOnly", "ContainersReadOnly"]
  }
}

resource "scaleway_iam_api_key" "registry_cleanup" {
  for_each           = local.registry_cleanup
  application_id     = scaleway_iam_application.registry_cleanup[each.key].id
  description        = "API key for app-${var.environment}-registry-cleanup"
  default_project_id = scaleway_account_project.project.id
}

resource "scaleway_secret" "registry_cleanup_secret_key" {
  for_each   = local.registry_cleanup
  name       = "registry-cleanup-secret-key"
  path       = "/registry-cleanup"
  project_id = scaleway_account_project.project.id
}

resource "scaleway_secret_version" "registry_cleanup_secret_key" {
  for_each  = local.registry_cleanup
  secret_id = scaleway_secret.registry_cleanup_secret_key[each.key].id
  data      = scaleway_iam_api_key.registry_cleanup[each.key].secret_key
}

resource "scaleway_job_definition" "registry_cleanup" {
  for_each     = local.registry_cleanup
  name         = "registry-cleanup-${var.environment}"
  cpu_limit    = 250
  memory_limit = 256
  image_uri    = "python:3.12-alpine"
  project_id   = scaleway_account_project.project.id
  region       = "fr-par"
  timeout      = "60m"

  # Standard library only: the script is passed inline, no image to build.
  startup_command = ["python", "-c", file("${path.module}/scripts/registry_cleanup.py")]

  cron {
    schedule = var.registry_cleanup_cron
    timezone = "Europe/Paris"
  }

  env = {
    SCW_DEFAULT_ORGANIZATION_ID = scaleway_account_project.project.organization_id
    REGISTRY_NAMESPACE_ID       = data.scaleway_registry_namespace.cleanup[each.key].namespace_id
    REGION                      = "fr-par"
    KEEP                        = tostring(var.registry_cleanup_keep)
    PROTECTED_TAGS              = join(",", var.registry_cleanup_protected_tags)
    DRY_RUN                     = tostring(var.registry_cleanup_dry_run)
  }

  secret_reference {
    secret_id   = scaleway_secret.registry_cleanup_secret_key[each.key].id
    environment = "SCW_SECRET_KEY"
  }

  depends_on = [scaleway_secret_version.registry_cleanup_secret_key]
}
