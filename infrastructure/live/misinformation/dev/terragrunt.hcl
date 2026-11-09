include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../template"
}

locals {
  # Dev jobs run on Jan 1st only: they are launched by hand.
  countries = {
    belgium = {
      model_name             = "mistral-small-2603"
      labelstudio_project    = 1
      labelstudio_project_id = 1
      job_cron_schedule      = "47 5 1 1 *"
    }
    belgium-flanders = {
      model_name             = "mistral-small-2603"
      labelstudio_project    = 1
      labelstudio_project_id = 1
      job_cron_schedule      = "41 5 1 1 *"
    }
    germany = {
      model_name             = "mistral-small-2603"
      labelstudio_project    = 1
      labelstudio_project_id = 1
      job_cron_schedule      = "48 5 1 1 *"
    }
    poland = {
      model_name             = "mistral-small-2603"
      labelstudio_project    = 1
      labelstudio_project_id = 1
      job_cron_schedule      = "57 5 1 1 *"
    }
    spain = {
      model_name             = "mistral-small-2603"
      labelstudio_project    = 1
      labelstudio_project_id = 1
      job_cron_schedule      = "51 5 1 1 *"
    }
  }
}

inputs = {
  environment                = "dev"
  barometre_pg_instance_name = get_env("TF_VAR_barometre_pg_instance_name", "rdb-poc")
  image_tag                  = "latest"

  countries = local.countries

  # TF_VAR_<secret>_<country>, with "-" replaced by "_" (belgium-flanders → belgium_flanders)
  country_secrets = {
    for country in keys(local.countries) : country => {
      postgres_password_labelstudio = get_env("TF_VAR_postgres_password_labelstudio_${replace(country, "-", "_")}")
      labelstudio_admin_password    = get_env("TF_VAR_labelstudio_admin_password_${replace(country, "-", "_")}")
      labelstudio_user_token        = get_env("TF_VAR_labelstudio_user_token_${replace(country, "-", "_")}")
    }
  }

  mediatree_password          = get_env("TF_VAR_mediatree_password")
  openai_api_key              = get_env("TF_VAR_openai_api_key")
  mistral_api_key             = get_env("TF_VAR_mistral_api_key")
  postgres_password_barometre = get_env("TF_VAR_postgres_password_barometre")
  sentry_dsn                  = get_env("TF_VAR_sentry_dsn")
}
