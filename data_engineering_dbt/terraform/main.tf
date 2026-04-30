terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = "coffee-shop-updated"
}

# -------------------------
# DATASETS
# -------------------------
resource "google_bigquery_dataset" "raw" {
  dataset_id = "coffee_raw"
  location   = "US"
}

resource "google_bigquery_dataset" "dev" {
  dataset_id = "coffee_dev"
  location   = "US"
}

resource "google_bigquery_dataset" "prod" {
  dataset_id = "coffee_prod"
  location   = "US"
}

# -------------------------
# SERVICE ACCOUNT (THIS IS WHAT YOU ARE MISSING)
# -------------------------
resource "google_service_account" "pipeline_sa" {
  account_id   = "coffee-pipeline-sa"
  display_name = "Coffee Pipeline SA"
}

resource "google_project_iam_member" "bq_editor" {
  project = "coffee-shop-updated"
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:coffee-pipeline-sa@coffee-shop-updated.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "bq_job_user" {
  project = "coffee-shop-updated"
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:coffee-pipeline-sa@coffee-shop-updated.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "bq_metadata" {
  project = "coffee-shop-updated"
  role    = "roles/bigquery.metadataViewer"
  member  = "serviceAccount:coffee-pipeline-sa@coffee-shop-updated.iam.gserviceaccount.com"
}