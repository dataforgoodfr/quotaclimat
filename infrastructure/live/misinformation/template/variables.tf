variable "environment" {
  type = string
}

variable "subject" {
  type        = string
  default     = "climate"
  description = "Topic of the deployment, part of every resource name (climate-safeguards-<env>, safeguards-climate-<country>-<env>...)."
}

variable "barometre_project_name" {
  type    = string
  default = "barometre"
}

variable "barometre_pg_instance_name" {
  type    = string
  default = "rdb-poc"
}

variable "image_tag" {
  type        = string
  default     = "latest"
  description = "Tag of the label-misinformation image used by the jobs."
}

# Countries deployed in this environment, keyed by the country name used in
# resource names and in the job's COUNTRY variable (e.g. belgium-flanders).
variable "countries" {
  type = map(object({
    model_name                 = string
    llm_provider               = optional(string, "mistral")
    labelstudio_project        = number
    labelstudio_project_id     = number
    job_cron_schedule          = optional(string, "5 6 1 1 *")
    job_cron_schedule_timezone = optional(string, "Europe/Paris")
  }))
}

# Per-country secrets, same keys as var.countries.
variable "country_secrets" {
  type = map(object({
    postgres_password_labelstudio = string
    labelstudio_admin_password    = string
    labelstudio_user_token        = string
  }))
  sensitive = true
}

# Shared secrets

variable "mediatree_password" {
  type      = string
  sensitive = true
}

variable "openai_api_key" {
  type      = string
  sensitive = true
}

variable "mistral_api_key" {
  type      = string
  sensitive = true
}

variable "postgres_password_barometre" {
  type      = string
  sensitive = true
}

variable "sentry_dsn" {
  type      = string
  sensitive = true
}
