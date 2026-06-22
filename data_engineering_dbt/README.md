# Coffee Shop Data Platform

## Overview

This project implements an end-to-end ELT data platform for a coffee shop business using:

- **Google Cloud Platform (GCP)**
- **BigQuery** for data warehousing
- **Terraform** for infrastructure management
- **Python CLI** for data ingestion
- **dbt** for data transformation and testing
- **Airflow** for orchestration
- **GitHub Actions** for CI/CD
- **Looker Studio** for reporting and dashboards

The platform follows a modern ELT architecture where raw data is first loaded into BigQuery and then transformed using dbt.

## Architecture

CSV Files
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

## Project Structure

data_engineering_dbt
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

## BigQuery Environment Structure

**Raw Layer**

Raw source data is loaded into:

coffee_raw
├── raw_astoria
├── raw_hells_kitchen
└── raw_lower_manhattan

**Development Environment**

dbt transformations are executed locally into:

coffee_dev
├── staging_astoria
├── staging_hells_kitchen
├── staging_lower_manhattan
├── int_sales
├── int_combined_stores
├── calendar
├── mart_bi
└── mart_ml

**Production Environment**

Production models are deployed to:

coffee_prod
├── staging_astoria
├── staging_hells_kitchen
├── staging_lower_manhattan
├── int_sales
├── int_combined_stores
├── calendar
├── mart_bi
└── mart_ml

## Data Pipeline Flow

**1. Infrastructure Provisioning**

Terraform creates and manages:

- BigQuery datasets
- BigQuery tables
- IAM permissions

New datasets can be added through Terraform as the project grows.

**2. Data Ingestion**

Raw CSV files are loaded into BigQuery using the custom Python CLI ingestion framework.

Target:

coffee_raw

**3. Data Transformation (dbt)**

dbt performs:

**Data Cleaning**
- Standardized column names
- Data type corrections
- Date conversions
**Data Quality Testing**
- Null tests on transaction_id
- Source validation
**Data Standardization**
- Deduplication
- Consistent date formatting
- Store consolidation

**4. Data Marts**

Final marts include:

| Mart    | Purpose                        |
| ------- | ------------------------------ |
| mart_bi | Reporting and dashboards       |
| mart_ml | Machine learning and analytics |

These marts are connected directly to Looker Studio.

**5. Deployment**

**Development**

Running dbt locally updates:

coffee_dev

**Production**

GitHub Actions automatically:

1. Detect merge into main branch
2. Run dbt
3. Deploy models to:

coffee_prod

**6. Orchestration**

Airflow runs daily and:

1. Loads new source data
2. Executes dbt models
3. Updates production marts
4. Refreshes reporting datasets

## CI/CD

GitHub Actions automates deployment.

Workflow:

Developer
    │
    ▼
Git Push
    │
    ▼
Pull Request
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

## Reporting

Looker Studio dashboards connect directly to:

coffee_prod.mart_bi

ML models can be built using data from:

coffee_prod.mart_ml

## Future Enhancements
- Incremental dbt models
- Data freshness monitoring
- Automated anomaly detection





