resource "scaleway_object_bucket" "kestra_storage" {
  name       = "kestra-${var.environment}"
  region     = "fr-par"
  project_id = data.scaleway_account_project.barometre.id
}
