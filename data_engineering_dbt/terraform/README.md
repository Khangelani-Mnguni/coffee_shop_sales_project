# Terraform Infrastructure

## Overview

This project uses [Terraform](#infrastructure-components) to provision and manage Google Cloud Platform (GCP) resources required by the Coffee Shop Data Platform.

Terraform ensures that infrastructure is:

- Version controlled
- Reproducible
- Scalable
- Consistent across environments

### Related Sections

- [Project Structure](#project-structure)
- [Infrastructure Components](#infrastructure-components)
- [Prerequisites](#prerequisites)
- [Authentication](#authentication)
- [Terraform Workflow](#terraform-workflow)
- [Local Setup](#local-setup)
- [Common Terraform Commands](#common-terraform-commands)
- [Infrastructure Environments](#infrastructure-environments)
- [Adding New Datasets](#adding-new-datasets)
- [Deployment Process](#deployment-process)
- [State Management](#state-management)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)
- [Future Enhancements](#future-enhancements)

---

# Project Structure

```text
terraform/
│
├── main.tf
├── variables.tf
├── outputs.tf
├── providers.tf
├── datasets.tf
├── terraform.tfvars
└── README.md
```

---

# Infrastructure Components

Terraform currently manages the following resources.

## BigQuery Datasets

```text
coffee_raw
coffee_dev
coffee_prod
```

### Purpose

```text
coffee_raw   → Raw ingested source data
coffee_dev   → Development dbt models
coffee_prod  → Production dbt models
```

## IAM Permissions

Terraform manages:

- Service account permissions
- BigQuery dataset access
- Project-level roles

## Future Resources

Terraform can be extended to provision:

- Cloud Storage Buckets
- Cloud Run Services
- Cloud Functions
- Airflow Infrastructure
- Monitoring Resources
- Service Accounts

---

# Prerequisites

Before using Terraform, install:

- [Terraform](https://developer.hashicorp.com/terraform)
- [Google Cloud SDK](https://cloud.google.com/sdk)

Verify Terraform installation:

```bash
terraform version
```

Verify Google Cloud SDK installation:

```bash
gcloud version
```

---

# Authentication

Authenticate with Google Cloud before running Terraform.

## Login

```bash
gcloud auth login
```

## Set Project

```bash
gcloud config set project <project-id>
```

## Authenticate Service Account

```bash
gcloud auth activate-service-account \
  --key-file=/path/to/service-account.json
```

## Verify Authentication

```bash
gcloud auth list
```

---

# Terraform Workflow

Terraform follows the standard Infrastructure as Code lifecycle.

```text
Write Configuration
        │
        ▼
terraform fmt
        │
        ▼
terraform validate
        │
        ▼
terraform plan
        │
        ▼
terraform apply
        │
        ▼
Infrastructure Updated
```

---

# Local Setup

## Clone Repository

```bash
git clone <repository-url>
```

```bash
cd terraform
```

## Initialize Terraform

```bash
terraform init
```

This downloads:

- Terraform providers
- Google provider plugins
- Required dependencies

---

# Common Terraform Commands

## Initialize Project

```bash
terraform init
```

## Format Configuration Files

```bash
terraform fmt
```

## Validate Configuration

```bash
terraform validate
```

## Preview Changes

```bash
terraform plan
```

## Apply Changes

```bash
terraform apply
```

## Auto Approve Apply

```bash
terraform apply -auto-approve
```

## View Current State

```bash
terraform show
```

## List Managed Resources

```bash
terraform state list
```

## Destroy Infrastructure

```bash
terraform destroy
```

## Auto Approve Destroy

```bash
terraform destroy -auto-approve
```

---

# Infrastructure Environments

Terraform provisions the datasets used throughout the platform.

```text
coffee_raw
     │
     ▼
coffee_dev
     │
     ▼
coffee_prod
```

### Environment Usage

| Dataset | Purpose |
|----------|----------|
| `coffee_raw` | Raw ingestion layer |
| `coffee_dev` | Local dbt development |
| `coffee_prod` | Production analytics |

---

# Adding New Datasets

New datasets should always be created through Terraform.

## Example Dataset

```hcl
resource "google_bigquery_dataset" "coffee_sandbox" {
  dataset_id = "coffee_sandbox"
  location   = "EU"
}
```

## Validate

```bash
terraform validate
```

## Preview Changes

```bash
terraform plan
```

## Apply Changes

```bash
terraform apply
```

---

# Deployment Process

Infrastructure updates follow the Git workflow.

```text
Developer
    │
    ▼
Update Terraform Code
    │
    ▼
terraform plan
    │
    ▼
Pull Request
    │
    ▼
Code Review
    │
    ▼
Merge to Main
    │
    ▼
Infrastructure Deployment
```

This ensures all infrastructure changes are:

- Reviewed
- Auditable
- Version controlled

---

# State Management

Terraform tracks deployed infrastructure using state files.

## View State

```bash
terraform show
```

## List Resources

```bash
terraform state list
```

## Show Specific Resource

```bash
terraform state show <resource-name>
```

Example:

```bash
terraform state show google_bigquery_dataset.coffee_dev
```

---

# Troubleshooting

## Reinitialize Providers

```bash
terraform init -upgrade
```

## Refresh State

```bash
terraform refresh
```

## Validate Configuration

```bash
terraform validate
```

## Debug Plan

```bash
terraform plan
```

---

# Best Practices

- Never create production datasets manually
- Manage all infrastructure through Terraform
- Review `terraform plan` before applying changes
- Use Git for version control
- Keep infrastructure changes small and reviewable
- Store service account credentials securely

---

# Future Enhancements

- Remote Terraform state storage
- State locking
- Environment-specific variables
- Automated infrastructure deployment via [GitHub Actions](https://github.com/features/actions)
- Monitoring and alerting resources
- Cloud Storage provisioning
- Cloud Run provisioning
- IAM automation
