locals {
  # Path layout relative to this file: <project>/<env>
  # e.g. advertising/dev  →  path_parts[0]="advertising", path_parts[1]="dev"
  path_parts  = split("/", path_relative_to_include())
  project     = local.path_parts[0]
  environment = local.path_parts[1]
}

# Use OpenTofu instead of Terraform.
terraform_binary = "tofu"

# Generate backend.tf in each leaf directory at plan/apply time.
# key preserves the existing dev/terraform.tfstate and prod/terraform.tfstate
# paths so no state migration is needed.
generate "backend" {
  path      = "backend.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<-EOF
    terraform {
      required_providers {
        scaleway = {
          source  = "scaleway/scaleway"
          version = "~> 2.76"
        }
        time = {
          source  = "hashicorp/time"
          version = "~> 0.12"
        }
      }
      backend "s3" {
        bucket                      = "quotaclimat-terraform-states"
        key                         = "${local.project}/${local.environment}/terraform.tfstate"
        endpoint                    = "https://s3.fr-par.scw.cloud"
        region                      = "fr-par"
        use_lockfile                = true
        skip_credentials_validation = true
        skip_region_validation      = true
        skip_requesting_account_id  = true
      }
    }
  EOF
}
