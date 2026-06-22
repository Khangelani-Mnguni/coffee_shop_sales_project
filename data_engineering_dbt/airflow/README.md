# Airflow Orchestration

## Overview

This module uses [Apache Airflow](https://airflow.apache.org/) to orchestrate the end-to-end ELT pipeline.

Airflow is responsible for:

- Scheduling pipeline execution
- Monitoring task status
- Running ingestion jobs
- Executing dbt transformations
- Running dbt tests
- Updating production datasets

### Related Sections

- [Project Structure](#project-structure)
- [Pipeline Overview](#pipeline-overview)
- [DAG Responsibilities](#dag-responsibilities)
- [Local Setup](#local-setup)
- [Running Airflow](#running-airflow)
- [Manual DAG Execution](#manual-dag-execution)
- [Monitoring](#monitoring)
- [Future Enhancements](#future-enhancements)

---

# Project Structure

```text
airflow/
│
├── dags/
│   └── coffee_pipeline.py
│
├── plugins/
│
├── logs/
│
└── README.md
```

---

# Pipeline Overview

The Airflow DAG orchestrates the complete data pipeline.

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

---

# DAG Responsibilities

## Data Ingestion

Loads source files into:

```text
coffee_raw
```

## Data Transformation

Executes dbt models.

```bash
dbt run
```

## Data Quality Testing

Executes dbt tests.

```bash
dbt test
```

Current validation includes:

- Null transaction ID checks
- Model integrity checks
- Source validation

## Production Refresh

Updates:

```text
coffee_prod
```

with the latest transformed data.

---

# Schedule

The DAG runs daily.

```text
Frequency: Daily
Environment: Production
Target Dataset: coffee_prod
```

---

# Local Setup

## Install Airflow

```bash
pip install apache-airflow
```

Verify installation:

```bash
airflow version
```

---

# Running Airflow

## Initialize Database

```bash
airflow db migrate
```

## Create User

```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

## Start Scheduler

```bash
airflow scheduler
```

## Start Webserver

```bash
airflow webserver --port 8080
```

---

# Manual DAG Execution

List available DAGs:

```bash
airflow dags list
```

Trigger pipeline manually:

```bash
airflow dags trigger coffee_pipeline
```

View DAG state:

```bash
airflow dags state coffee_pipeline
```

---

# Monitoring

Airflow provides:

- DAG execution history
- Task execution logs
- Failure monitoring
- Retry handling
- Schedule tracking

The Airflow UI is available at:

```text
http://localhost:8080
```

---

# Production Workflow

```text
Airflow Scheduler
        │
        ▼
Load New Data
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
Refresh Dashboards
```

---

# Dependencies

Airflow integrates with:

- [BigQuery](../README.md#bigquery-environment-structure)
- [dbt](../coffee_project/README.md)
- [Terraform](../terraform/README.md)
- [Looker Studio](../README.md#reporting)

---

# Troubleshooting

## Verify DAGs

```bash
airflow dags list
```

## Verify Tasks

```bash
airflow tasks list coffee_pipeline
```

## Check Scheduler

```bash
airflow scheduler
```

## View Logs

```bash
airflow tasks logs coffee_pipeline
```

---

# Future Enhancements

- Email alerts
- Slack notifications
- Data freshness monitoring
- Automatic retries
- SLA monitoring
- Dynamic task generation
- Environment-specific DAG configurations
- Data observability integration
