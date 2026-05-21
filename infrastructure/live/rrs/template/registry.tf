resource "scaleway_registry_namespace" "rrs" {
  name       = "rrs-${var.environment}"
  project_id = scaleway_account_project.project.id
  region     = "fr-par"
  is_public  = false
}
