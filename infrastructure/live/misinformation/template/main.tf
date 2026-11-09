# Misinformation labelling (formerly climateguard "climate safeguards").
#
# One Scaleway project per environment (climate-safeguards-<env>) holding, for
# every country of var.countries, a Label Studio container (database on the
# barometre RDB instance), an output bucket, an IAM application/key and the
# label-misinformation job definition.
#
# Resource names are kept identical to the climateguard ones: renaming them
# would destroy and recreate the resources (buckets, databases, secrets that
# Kestra reads).

locals {
  project_name = "${var.subject}-safeguards-${var.environment}"
}

resource "scaleway_account_project" "project" {
  name = local.project_name
}

data "scaleway_account_project" "barometre" {
  name = var.barometre_project_name
}
