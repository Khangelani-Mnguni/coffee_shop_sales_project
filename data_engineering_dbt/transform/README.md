# dbt Project

## Overview

This project uses [dbt](#data-transformation-flow) to transform raw coffee shop transaction data stored in BigQuery into clean, analytics-ready datasets.

dbt is responsible for:

- Data cleaning
- Data standardization
- Data quality testing
- Business logic implementation
- Building reporting and machine learning data marts

### Related Sections

- [Project Structure](#project-structure)
- [Model Architecture](#model-architecture)
- [Sources](#sources)
- [Staging Models](#staging-models)
- [Intermediate Models](#intermediate-models)
- [Shared Dimensions](#shared-dimensions)
- [Data Marts](#data-marts)
- [Data Transformation Flow](#data-transformation-flow)
- [Data Quality Testing](#data-quality-testing)
- [Local Setup](#local-setup)
- [Connecting dbt on a New Local Machine](#connecting-dbt-on-a-new-local-machine)
- [Common dbt Commands](#common-dbt-commands)
- [Development Workflow](#development-workflow)
- [Production Deployment](#production-deployment)
- [Documentation](#documentation)

---

# Project Structure

```text
coffee_project/
│
├── analyses/
├── logs/
├── macros/
│
├── models/
│   ├── common/
│   │   └── calendar.sql
│   │
│   ├── staging/
│   │   ├── staging_astoria.sql
│   │   ├── staging_hells_kitchen.sql
│   │   └── staging_lower_manhattan.sql
│   │
│   ├── intermediate/
│   │   ├── int_sales.sql
│   │   ├── int_combined_stores.sql
│   │   └── intermediary.yml
│   │
│   └── marts/
│       ├── mart_bi.sql
│       ├── mart_ml.sql
│       └── marts.yml
│
├── sources.yml
├── dbt_project.yml
└── profiles.yml
```

---

# Model Architecture

The dbt project follows a layered architecture.

```text
Raw Sources
     │
     ▼
Staging Models
     │
     ▼
Intermediate Models
     │
     ▼
Dimensions
     │
     ▼
Data Marts
```

---

# Sources

Raw source tables originate from the BigQuery dataset:

```text
coffee_raw
├── raw_astoria
├── raw_hells_kitchen
└── raw_lower_manhattan
```

These source tables are defined in:

```text
sources.yml
```

---

# Staging Models

The staging layer performs initial cleaning and standardization.

## Models

```text
staging_astoria
staging_hells_kitchen
staging_lower_manhattan
```

## Responsibilities

- Standardize column names
- Convert data types
- Format dates
- Remove duplicate records
- Prepare data for downstream transformations

## Output Datasets

```text
coffee_dev
```

or

```text
coffee_prod
```

depending on the deployment target.

---

# Intermediate Models

The intermediate layer contains reusable business logic.

## Models

```text
int_sales
int_combined_stores
```

## Responsibilities

- Consolidate all store transactions
- Apply business rules
- Create reusable analytical datasets
- Simplify downstream reporting

---

# Shared Dimensions

Shared dimensions provide reusable lookup tables across marts.

## Models

```text
calendar
```

## Responsibilities

- Date dimension generation
- Reporting date hierarchies
- Time-series analysis support

---

# Data Marts

The marts layer contains business-facing datasets.

## Models

```text
mart_bi
mart_ml
```

## Purpose

### mart_bi

Used for:

- [Looker Studio](#data-marts) dashboards
- Business reporting
- KPI monitoring

### mart_ml

Used for:

- Machine learning
- Predictive analytics
- Forecasting models

---

# Data Transformation Flow

```text
coffee_raw
     │
     ▼
staging_*
     │
     ▼
int_sales
int_combined_stores
     │
     ▼
calendar
     │
     ▼
mart_bi
mart_ml
```

---

# Data Quality Testing

dbt tests ensure data quality before models are promoted.

## Current Tests

### transaction_id

```text
NOT NULL
```

Additional tests can be added for:

- Uniqueness
- Accepted values
- Relationships
- Freshness

---

# Local Setup

## Prerequisites

Install dbt BigQuery:

```bash
pip install dbt-bigquery
```

Verify installation:

```bash
dbt --version
```

---

# Authentication

Download the Google Cloud service account key.

Set credentials:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Verify authentication:

```bash
gcloud auth activate-service-account --key-file=/path/to/service-account.json
```

---

# Configure profiles.yml

Location:

```text
~/.dbt/profiles.yml
```

Example:

```yaml
coffee_project:
  target: dev

  outputs:
    dev:
      type: bigquery
      method: service-account
      project: coffee-shop-updated
      dataset: coffee_dev
      keyfile: /path/to/service-account.json
      threads: 4
```

---

# Connecting dbt on a New Local Machine

## Clone Repository

```bash
git clone <repository-url>
```

```bash
cd coffee_project
```

## Create Virtual Environment

```bash
python -m venv .venv
```

### Mac/Linux

```bash
source .venv/bin/activate
```

### Windows

```bash
.venv\Scripts\activate
```

## Install Dependencies

```bash
pip install dbt-bigquery
```

## Configure Credentials

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

## Verify Connection

```bash
dbt debug
```

---

# Common dbt Commands

## Install Packages

```bash
dbt deps
```

## Verify Connection

```bash
dbt debug
```

## Run Entire Project

```bash
dbt run
```

## Build Entire Project

```bash
dbt build
```

## Run Tests

```bash
dbt test
```

## Run Staging Models

```bash
dbt run --select staging
```

## Run Intermediate Models

```bash
dbt run --select intermediate
```

## Run Marts

```bash
dbt run --select marts
```

## Run Single Model

```bash
dbt run --select mart_bi
```

## Generate Documentation

```bash
dbt docs generate
```

## Serve Documentation

```bash
dbt docs serve
```

---

# Development Workflow

Local development updates:

```text
coffee_dev
```

Typical workflow:

```bash
dbt run
dbt test
dbt docs generate
```

---

# Production Deployment

Production deployment is automated through [GitHub Actions](#production-deployment).

Workflow:

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

Production models are materialized into:

```text
coffee_prod
```

---

# Documentation

Generate documentation:

```bash
dbt docs generate
```

Launch documentation locally:

```bash
dbt docs serve
```

Documentation includes:

- Model lineage
- Source dependencies
- Column descriptions
- Test coverage

---

# Future Enhancements

- Incremental models
- Source freshness monitoring
- Additional data quality tests
- Snapshot implementation
- Exposure definitions
- Automated documentation publishing
- CI/CD validation testing
