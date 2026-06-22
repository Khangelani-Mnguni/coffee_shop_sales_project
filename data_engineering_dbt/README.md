# Project Overview

This project is an end-to-end data engineering platform for coffee shop sales data built using a modern ELT architecture.

The platform leverages:

- [BigQuery](#bigquery-environment-structure) for data warehousing
- [Terraform](#infrastructure-provisioning) for infrastructure management
- [Python CLI](#data-ingestion) for data ingestion
- [dbt](#data-transformation-dbt) for data transformation and testing
- [Apache Airflow](#orchestration) for orchestration
- [GitHub Actions](#cicd-workflow) for CI/CD
- [Looker Studio](#reporting) for reporting and dashboards

The pipeline loads raw transactional data into BigQuery, transforms it using dbt, and exposes analytics-ready datasets for reporting and machine learning use cases.

---

# Architecture

```text
Raw CSV Files
    │
    ▼
Python CLI Ingestion
    │
    ▼
coffee_raw (BigQuery)
    │
    ▼
dbt Staging Models
    │
    ▼
dbt Intermediate Models
    │
    ▼
Data Marts
    │
    ├── Looker Studio Dashboards
    │
    └── Machine Learning Models
```

---

# Repository Structure

```text
data_engineering_dbt/
│
├── airflow/
│   └── DAGs and orchestration
│
├── ingestion/
│   ├── cli.py
│   ├── loader.py
│   ├── schema.py
│   └── config.yaml
│
├── terraform/
│   └── BigQuery infrastructure
│
├── coffee_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   ├── common/
│   │   └── marts/
│   │
│   ├── macros/
│   ├── analyses/
│   ├── logs/
│   ├── dbt_project.yml
│   └── profiles.yml
│
└── data/
    ├── Astoria.csv
    ├── Hells_Kitchen.csv
    └── Lower_Manhattan.csv
```

---

# BigQuery Environment Structure

The project follows a multi-layer BigQuery architecture consisting of Raw, Development, and Production environments.

## Raw Layer

Raw source data is loaded directly into BigQuery through the ingestion framework.

```text
coffee_raw
├── raw_astoria
├── raw_hells_kitchen
└── raw_lower_manhattan
```

## Development Environment

All local dbt development work is materialized into the development dataset.

```text
coffee_dev
├── staging_astoria
├── staging_hells_kitchen
├── staging_lower_manhattan
├── int_sales
├── int_combined_stores
├── calendar
├── mart_bi
└── mart_ml
```

## Production Environment

Production models are deployed automatically through GitHub Actions and Airflow.

```text
coffee_prod
├── staging_astoria
├── staging_hells_kitchen
├── staging_lower_manhattan
├── int_sales
├── int_combined_stores
├── calendar
├── mart_bi
└── mart_ml
```

---

# Infrastructure Provisioning

Infrastructure is managed using Terraform.

Terraform is responsible for:

- Creating BigQuery datasets
- Managing BigQuery resources
- Managing IAM permissions
- Supporting future dataset additions

New datasets should always be added through Terraform to ensure infrastructure remains version-controlled and reproducible.

---

# Data Ingestion

Raw CSV files are loaded into BigQuery using the custom Python CLI ingestion framework.

### Source Files

```text
data/
├── Astoria.csv
├── Hells_Kitchen.csv
└── Lower_Manhattan.csv
```

### Target Dataset

```text
coffee_raw
```

### Ingestion Flow

```text
CSV Files
    │
    ▼
Python CLI
    │
    ▼
BigQuery Raw Tables
    │
    ▼
coffee_raw Dataset
```

---

# Data Transformation (dbt)

dbt is responsible for transforming raw transactional data into clean, analytics-ready datasets.

### Data Cleaning

- Standardized column names
- Data type corrections
- Date formatting and conversion
- Null handling

### Data Quality Testing

- Null tests on `transaction_id`
- Source validation
- Model integrity checks

### Data Standardization

- Deduplication of transactions
- Consistent date formatting
- Business logic implementation
- Store consolidation

### Model Layers

#### Staging Models

```text
staging_astoria
staging_hells_kitchen
staging_lower_manhattan
```

#### Intermediate Models

```text
int_sales
int_combined_stores
```

#### Shared Dimensions

```text
calendar
```

#### Data Marts

```text
mart_bi
mart_ml
```

---

# Data Marts

The final marts are designed for reporting and advanced analytics.

```text
mart_bi  → Business Intelligence & Reporting
mart_ml  → Machine Learning & Predictive Analytics
```

These marts serve as the primary data source for downstream consumers.

---

# Deployment

## Development Workflow

When dbt is executed locally:

```bash
dbt run
```

Models are materialized into:

```text
coffee_dev
```

This allows developers to test and validate changes before deployment.

## Production Workflow

GitHub Actions automates production deployments.

```text
Developer
    │
    ▼
Git Push
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
GitHub Actions
    │
    ▼
dbt Run
    │
    ▼
coffee_prod Updated
```

Upon merge to the main branch:

1. GitHub Actions executes the deployment workflow
2. dbt models are run against production
3. Production datasets are updated automatically

---

# Orchestration

Apache Airflow orchestrates the entire ELT workflow.

The pipeline runs daily and performs:

1. Raw data ingestion
2. Data validation
3. dbt model execution
4. dbt testing
5. Production dataset refresh
6. Reporting layer updates

### Airflow Workflow

```text
Start
  │
  ▼
Load New Data
  │
  ▼
Validate Raw Tables
  │
  ▼
Run dbt Models
  │
  ▼
Run dbt Tests
  │
  ▼
Update coffee_prod
  │
  ▼
Refresh BI Layer
  │
  ▼
End
```

This ensures production reporting and analytical datasets remain up to date.

---

# CI/CD Workflow

GitHub Actions provides automated deployment and continuous integration.

### Workflow

```text
Developer
    │
    ▼
Git Push
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
GitHub Actions
    │
    ▼
Deploy dbt Models
    │
    ▼
Update coffee_prod
```

Benefits include:

- Automated deployments
- Version-controlled infrastructure
- Reduced manual intervention
- Consistent production releases

---

# Reporting

Looker Studio connects directly to the production business intelligence mart.

```text
coffee_prod.mart_bi
```

Reporting capabilities include:

- Store performance monitoring
- Revenue analysis
- Transaction analysis
- Trend reporting
- Operational dashboards

The machine learning mart can also be used for future predictive analytics and forecasting initiatives.

---

# Technologies Used

| Category | Technology |
|-----------|------------|
| Programming | Python |
| Data Warehouse | BigQuery |
| Infrastructure | Terraform |
| Data Transformation | dbt |
| Orchestration | Apache Airflow |
| CI/CD | GitHub Actions |
| Reporting | Looker Studio |
| Version Control | Git |

---

# Useful Commands

## dbt

### Install Dependencies

```bash
dbt deps
```

### Verify Connection

```bash
dbt debug
```

### Run Models

```bash
dbt run
```

### Run Tests

```bash
dbt test
```

### Build Entire Project

```bash
dbt build
```

### Generate Documentation

```bash
dbt docs generate
```

### Serve Documentation

```bash
dbt docs serve
```

---

## Terraform

### Initialize

```bash
terraform init
```

### Validate

```bash
terraform validate
```

### Format

```bash
terraform fmt
```

### Plan Changes

```bash
terraform plan
```

### Apply Changes

```bash
terraform apply
```

### Destroy Infrastructure

```bash
terraform destroy
```

---

## Airflow

### List DAGs

```bash
airflow dags list
```

### Trigger Pipeline

```bash
airflow dags trigger coffee_pipeline
```

---

# Future Enhancements

- Incremental dbt models
- Data freshness monitoring
- Automated anomaly detection
- Additional store locations
