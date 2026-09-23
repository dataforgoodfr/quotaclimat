# S3 bucket for storing Mediatree extended perimeter data.
# Read/write access is granted via the rrs-ci IAM policy in iam.tf.
resource "scaleway_object_bucket" "mediatree_extended_perimeter" {
  name       = "mediatree-extended-perimeter-${var.environment}"
  region     = "fr-par"
  project_id = scaleway_account_project.project.id
}

# S3 bucket for storing misinformation detection extended perimeter data.
# Read/write access is granted via the rrs-ci IAM policy in iam.tf.
resource "scaleway_object_bucket" "misinformation_extended_perimeter" {
  name       = "misinformation-extended-perimeter-${var.environment}"
  region     = "fr-par"
  project_id = scaleway_account_project.project.id
}
