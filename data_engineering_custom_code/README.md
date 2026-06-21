## Project Overview

This project is an end-to-end data engineering pipeline for a coffee shop's sales data. It handles both historical data backfilling and scheduled incremental updates. The pipeline extracts, transforms, and loads (ETL) data from raw sources into on-premise databases (MySQL, PostgreSQL) and a cloud data warehouse (BigQuery).

Key Features
Backdating & Historical Load: Python scripts clean and ingest large volumes of historical CSV data into databases.

Scheduled Incremental Updates: Lightweight, automated scripts process only recent sales records to prevent redundant data processing.

Event-Driven Cloud Integration: A processed file uploaded to Google Cloud Storage (GCS) automatically triggers a serverless Google Cloud Run function, loading the new data directly into BigQuery.

Orchestration: Apache Airflow (running in a Docker container on Linux/Ubuntu) is used to automate and orchestrate pipeline execution.

Business Intelligence: Data is visualized using Power BI (for on-premise MySQL/Postgres data) and Looker Studio (for BigQuery data).

# Repository Structure

The repository is structured to clearly separate orchestration, backdating, and scheduled environments:

- Images/
- airflow/
  - airflow_dag_csv_to_csv.py
  - airflow_dag_csv_to_mysql.py
  - airflow_dags_mysql_to_postgres.py
- backdating/
  - etl_backdating_bigquery_cloud_run_function.py
  - etl_backdating_csv_to_csv.py
  - etl_backdating_csv_to_mysql.py
  - etl_backdating_mysql_to_postgres.py
  - etl_backdating_synthetic_data_to_mysql.py
- data/
- development/
- scheduled/
  - scheduled_csv_to_csv.py
  - scheduled_csv_to_mysql.py
  - scheduled_mysql_to_postgres.py
- README.md
- requirements.txt

(Note: System folders like __pycache__ and .DS_Store are present in the repository but typically ignored in version control).

# Technologies Used

Programming: Python

Databases: MySQL (OLTP), PostgreSQL (OLAP), Google BigQuery

Cloud (GCP): Google Cloud Storage, Cloud Run

Orchestration & Infrastructure: Apache Airflow, Docker, Linux/Ubuntu

Business Intelligence: Power BI, Looker Studio

# How it Works
Backdating: Execution of backdating scripts (e.g., etl_backdating_csv_to_mysql.py) reads raw sales data, applies transformations, and bulk-loads it into the databases to establish the baseline dataset.

Scheduled Automation: Scheduled scripts process new records at regular intervals, acting as smaller, efficient versions of the backdating process.

Cloud Pipeline: An upload to a designated GCS bucket triggers a Cloud Run function. This function reads the new file and loads it into BigQuery for near-real-time analytics.

Dashboards: Looker Studio connects directly to BigQuery for highly scalable analytics, while Power BI hooks into the on-premise PostgreSQL/MySQL databases for transaction-level metrics.

Future Enhancements
Implement a robust error-handling and logging system across all ETL scripts.

Set up a data quality monitoring framework to ensure accuracy and consistency at each pipeline stage.
