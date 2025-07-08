# Copyright (c) 2025 Skyflow, Inc.

# -- PROVIDERS --
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}


# -- DATA --
data "google_project" "project" {}


# -- VARIABLES --
variable "gcp_project_id" {
  description = "The ID of your GCP project."
  type        = string
}

variable "gcp_region" {
  description = "The GCP region to deploy within (e.g., us-west1)."
  type        = string
  default     = "us-west1"
}

variable "cloud_run_service_name" {
  description = "Name of the Cloud Run service."
  type        = string
}

variable "cloud_run_service_sa_id" {
  description = "ID of the dedicated service account for the Cloud Run service."
  type        = string
}

variable "cloud_run_service_env_vars" {
  description = "Non-secret environment variables to attach to the Cloud Run service."
  type        = map(string)
}

variable "image_name" {
  description = "Name of the Docker image in GCR (without tag or project)."
  type        = string
}

variable "image_tag" {
  description = "Tag of the Docker image in GCR."
  type        = string
}

variable "bigquery_connection_id" {
  description = "ID of the connection between BigQuery and Cloud Run."
  type        = string
}

variable "skyflow_sa_credentials_secret_id" {
  description = "ID of the secret in Secret Manager containing credentials for the Skyflow service account."
  type        = string
}

variable "skyflow_sa_credentials_secret_value" {
  description = "JSON-formatted credentials for the Skyflow service account."
  type        = string
  sensitive   = true
}

variable "cloud_run_service_skyflow_sa_credentials_env_var_name" {
  description = "Name of the environment variable in Cloud Run containing credentials for the Skyflow service account."
  type        = string
  default     = "SKYFLOW_SA_CREDENTIALS"
}


# -- Enable Services --
resource "google_project_service" "bigquery" {
  project            = var.gcp_project_id
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "run" {
  project            = var.gcp_project_id
  service            = "run.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudbuild" {
  project            = var.gcp_project_id
  service            = "cloudbuild.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "iam" {
  project            = var.gcp_project_id
  service            = "iam.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "secretmanager" {
  project            = var.gcp_project_id
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "iamcredentials" {
  project            = var.gcp_project_id
  service            = "iamcredentials.googleapis.com"
  disable_on_destroy = false
}


# -- Service Accounts --
resource "google_service_account" "cloud_run_service_sa" {
  project      = var.gcp_project_id
  account_id   = var.cloud_run_service_sa_id
  display_name = "Dedicated SA for ${var.cloud_run_service_name}"
  description  = "Service Account used by the ${var.cloud_run_service_name} Cloud Run service."

  depends_on = [
    google_project_service.iam
  ]
}


# -- Secret Manager --
resource "google_secret_manager_secret" "skyflow_sa_credentials_secret" {
  project   = var.gcp_project_id
  secret_id = var.skyflow_sa_credentials_secret_id

  replication {
    user_managed {
      replicas {
        location = var.gcp_region
      }
    }
  }

  depends_on = [
    google_project_service.secretmanager
  ]
}

resource "google_secret_manager_secret_version" "skyflow_sa_credentials_secret_version" {
  secret      = google_secret_manager_secret.skyflow_sa_credentials_secret.id
  secret_data = var.skyflow_sa_credentials_secret_value
}

resource "google_secret_manager_secret_iam_member" "skyflow_sa_credentials_secret_accessor" {
  project   = google_secret_manager_secret.skyflow_sa_credentials_secret.project
  secret_id = google_secret_manager_secret.skyflow_sa_credentials_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.cloud_run_service_sa.email}"
}


# -- Deploy to Cloud Run --
resource "google_cloud_run_v2_service" "service" {
  project             = var.gcp_project_id
  name                = var.cloud_run_service_name
  location            = var.gcp_region
  deletion_protection = false
  ingress             = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.cloud_run_service_sa.email

    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }

    max_instance_request_concurrency = 40

    containers {
      image = "gcr.io/${var.gcp_project_id}/${var.image_name}:${var.image_tag}"
      ports {
        container_port = 8080
      }
      resources {
        limits = {
          "cpu"    = 1
          "memory" = "512Mi"
        }
        cpu_idle = true
        startup_cpu_boost = false
      }

      env {
        name = var.cloud_run_service_skyflow_sa_credentials_env_var_name
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.skyflow_sa_credentials_secret.secret_id
            version = "latest"
          }
        }
      }

      dynamic "env" {
        for_each = merge(var.cloud_run_service_env_vars, {
          "GCP_PROJECT_ID" = var.gcp_project_id
        })
        content {
          name  = env.key
          value = env.value
        }
      }
    }
  }

  depends_on = [
    google_project_service.run,
    google_secret_manager_secret_iam_member.skyflow_sa_credentials_secret_accessor,
    google_secret_manager_secret_version.skyflow_sa_credentials_secret_version
  ]
}


# -- BigQuery Connection --
resource "google_bigquery_connection" "connection" {
  project       = var.gcp_project_id
  connection_id = var.bigquery_connection_id
  location      = var.gcp_region
  cloud_resource {}

  depends_on = [
    google_project_service.bigquery
  ]
}

resource "google_cloud_run_service_iam_member" "bq_cloud_run_invoker" {
  project  = google_cloud_run_v2_service.service.project
  location = google_cloud_run_v2_service.service.location
  service  = google_cloud_run_v2_service.service.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_bigquery_connection.connection.cloud_resource[0].service_account_id}"
}


# -- Outputs --
output "cloud_run_service_url" {
  description = "The URL of the deployed Cloud Run service."
  value       = google_cloud_run_v2_service.service.uri
}

output "cloud_run_service_sa_email" {
  description = "Email of the dedicated service account for the Cloud Run service."
  value       = google_service_account.cloud_run_service_sa.email
}

output "bigquery_connection_id" {
  description = "The connection ID to use in BigQuery UDFs."
  value       = "${data.google_project.project.number}.${google_bigquery_connection.connection.location}.${google_bigquery_connection.connection.connection_id}"
}

output "bigquery_connection_sa" {
  description = "The service account created for the BigQuery connection."
  value       = "serviceAccount:${google_bigquery_connection.connection.cloud_resource[0].service_account_id}"
}
