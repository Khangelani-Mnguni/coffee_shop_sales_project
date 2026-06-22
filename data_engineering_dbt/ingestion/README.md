# Data Ingestion

## Overview

This module is responsible for loading raw coffee shop transaction data into BigQuery.

The ingestion framework uses a custom [Python CLI](#running-ingestion) to load source CSV files into the raw BigQuery layer before downstream transformations are performed by [dbt](../coffee_project/README.md).

### Related Sections

- [Project Structure](#project-structure)
- [Source Data](#source-data)
- [Target Dataset](#target-dataset)
- [Ingestion Flow](#ingestion-flow)
- [Local Setup](#local-setup)
- [Running Ingestion](#running-ingestion)
- [Validation](#validation)
- [Troubleshooting](#troubleshooting)
- [Future Enhancements](#future-enhancements)

---

# Project Structure

```text
ingestion/
│
├── cli.py
├── loader.py
├── schema.py
├── config.yaml
└── README.md
```

---

# Source Data

The ingestion process loads source CSV files from:

```text
data/
├── Astoria.csv
├── Hells_Kitchen.csv
└── Lower_Manhattan.csv
```

### Store Coverage

| File | Store |
|--------|--------|
| Astoria.csv | Astoria |
| Hells_Kitchen.csv | Hells Kitchen |
| Lower_Manhattan.csv | Lower Manhattan |

---

# Target Dataset

Raw data is loaded into the BigQuery dataset:

```text
coffee_raw
```

### Destination Tables

```text
coffee_raw
├── raw_astoria
├── raw_hells_kitchen
└── raw_lower_manhattan
```

---

# Ingestion Flow

```text
CSV Files
    │
    ▼
Python CLI
    │
    ▼
Schema Validation
    │
    ▼
BigQuery Load
    │
    ▼
coffee_raw
```

---

# Local Setup

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
pip install -r requirements.txt
```

---

# Authentication

Authenticate using your Google Cloud service account.

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Verify authentication:

```bash
gcloud auth activate-service-account \
  --key-file=/path/to/service-account.json
```

---

# Running Ingestion

## Astoria

```bash
python cli.py --store astoria
```

## Hells Kitchen

```bash
python cli.py --store hells_kitchen
```

## Lower Manhattan

```bash
python cli.py --store lower_manhattan
```

---

# Validation

After loading data, validate row counts.

## Astoria

```sql
SELECT COUNT(*)
FROM coffee_raw.raw_astoria;
```

## Hells Kitchen

```sql
SELECT COUNT(*)
FROM coffee_raw.raw_hells_kitchen;
```

## Lower Manhattan

```sql
SELECT COUNT(*)
FROM coffee_raw.raw_lower_manhattan;
```

---

# Expected Output

Successful ingestion creates:

```text
coffee_raw
├── raw_astoria
├── raw_hells_kitchen
└── raw_lower_manhattan
```

These tables serve as source inputs for [dbt](../coffee_project/README.md) transformations.

---

# Troubleshooting

## Verify Credentials

```bash
gcloud auth list
```

## Verify Project

```bash
gcloud config list
```

## Verify BigQuery Access

```bash
bq ls
```

---

# Future Enhancements

- Incremental ingestion support
- Automated schema evolution
- Source file validation
- Data quality checks before loading
- Automated ingestion logging
- Error notification framework
