# TEMP (carried over from climateguard): direct bucket access for one user.
data "scaleway_iam_user" "user" {
  email = "giuseppe@dataforgood.fr"
}

resource "scaleway_object_bucket" "bucket" {
  for_each = var.countries

  name       = "safeguards-${var.subject}-${each.key}-${var.environment}"
  project_id = scaleway_account_project.project.id
}

resource "scaleway_object_bucket_policy" "bucket_policy" {
  for_each = var.countries

  project_id = scaleway_account_project.project.id
  bucket     = scaleway_object_bucket.bucket[each.key].name
  policy = jsonencode({
    Version = "2023-04-17",
    Id      = "ApplicationAccessPolicy",
    Statement = [
      {
        Effect    = "Allow"
        Action    = ["s3:*"]
        Principal = { SCW = "application_id:${scaleway_iam_application.project_application[each.key].id}" }
        Resource = [
          scaleway_object_bucket.bucket[each.key].name,
          "${scaleway_object_bucket.bucket[each.key].name}/*",
        ]
      },
      # TEMP (carried over from climateguard)
      {
        Effect    = "Allow"
        Action    = ["s3:*"]
        Principal = { SCW = "user_id:${data.scaleway_iam_user.user.id}" }
        Resource = [
          scaleway_object_bucket.bucket[each.key].name,
          "${scaleway_object_bucket.bucket[each.key].name}/*",
        ]
      },
    ]
  })
}
