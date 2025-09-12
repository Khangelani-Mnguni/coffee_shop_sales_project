Project Title: Coffee Shop Data Engineering Pipeline
Project Overview
This project showcases a complete end-to-end data engineering pipeline for a coffee shop's sales data. The pipeline is designed to handle historical data backfilling and regular, scheduled updates. It includes data extraction, transformation, and loading (ETL) processes that move data from various sources to different destinations, including on-premise databases and a cloud data warehouse.

The core of this project focuses on automating data ingestion, ensuring data consistency, and providing a robust foundation for business intelligence and analytics. A key aspect of this project's development was the use of Large Language Models (LLMs) like ChatGPT and Gemini to generate comprehensive Python scripts, significantly accelerating the development process.

Key Features
Backdating Scripts: A series of Python scripts to ingest large volumes of historical data from a raw CSV file. These scripts are essential for populating databases and data warehouses with past sales information.

CSV to CSV: A script to process and transform raw data into a cleaned, processed CSV format.

CSV to MySQL: A pipeline that takes the raw CSV data, transforms it, and loads it into a MySQL OLTP (Online Transaction Processing) database. 

MySQL to PostgreSQL: It then extracts, transforms, and loads that data into a PostgreSQL OLAP (Online Analytical Processing) database, which is optimized for analytics.

Scheduled Scripts: Automated scripts designed to handle incremental updates. These can be scheduled with tools like Apache Airflow or cron jobs to regularly process new data.

Cloud Integration (GCP):

Google Cloud Storage (GCS): Used as a landing zone for processed data, such as transformed.csv.

Cloud Run: A serverless compute platform used to trigger a data loading process into BigQuery whenever a new processed file is uploaded to the GCS bucket. This makes the pipeline event-driven and scalable.

Orchestration with Airflow: Apache Airflow is used to orchestrate and automate the data pipelines. The Airflow instance was run in a Docker container on a Linux/Ubuntu environment, providing a scalable and portable setup.

Business Intelligence:

Looker Studio (formerly Google Data Studio): Data from BigQuery is pulled into Looker Studio to create an interactive dashboard for visualizing key performance indicators (KPIs) and sales trends.

Power BI: Another dashboard created to visualize data from the on-premise databases (MySQL/Postgres), providing a different analytics view.

Repository Structure
The repository is organized to clearly separate the different components of the data pipeline.

├── Coffee Shop Sales2.csv          # Raw sales data
├── processed_sales_data.csv       # Processed sales data
│
├── backdating_scripts/            # Scripts for historical data ingestion
│   ├── etl_backdating_bigquery_cloud_run_function.py  # Cloud Run script for BigQuery
│   ├── etl_backdating_csv_to_csv.py       # Raw CSV to processed CSV
│   ├── etl_backdating_csv_to_mysql.py     # Raw CSV to MySQL
│   ├── etl_backdating_mysql_to_postgres.py# MySQL to Postgres
│   └── etl_backdating_synthetic_data_to_mysql.py # Script for generating synthetic data
│
├── scheduled_scripts/             # Scripts for automated, scheduled updates
│   ├── scheduled_csv_to_csv.py
│   ├── scheduled_csv_to_mysql.py
│   └── scheduled_mysql_to_postgres.py
│
├── airflow_dags/                  # Example Apache Airflow DAGs
│   ├── airflow_dag_csv_to_csv.py
│   ├── airflow_dags_mysql_to_postgres.py
│   └── ...
│
└── requirements.txt               # Python dependencies
Technologies Used
Programming Language: Python 🐍

Databases:

MySQL (OLTP)

PostgreSQL (OLAP)

BigQuery (Cloud Data Warehouse)

Cloud Services:

Google Cloud Platform (GCP)

Google Cloud Storage

Cloud Run

Scheduling/Orchestration:

Apache Airflow

Docker 🐳 (for Airflow)

Linux/Ubuntu

Business Intelligence Tools:

Power BI

Looker Studio

AI/LLM Tools:

ChatGPT

Gemini

Version Control: Git / GitHub

How it Works
1. Backdating Process
To populate the databases with historical data, the backdating scripts are executed. For example, etl_backdating_csv_to_mysql.py reads the raw Coffee Shop Sales2.csv, performs necessary transformations (e.g., data type conversions, cleaning), and then bulk loads the data into the MySQL database. The same logic applies to the other backdating scripts, ensuring a consistent and complete dataset.

2. Scheduled Automation
The scheduled scripts are designed to run at regular intervals to handle new data. They are smaller, more efficient versions of the backdating scripts that only process recent sales records, preventing redundant data processing.

3. Cloud-Based Pipeline
This part of the project demonstrates a modern, event-driven architecture.

A processed file (transformed1.csv) is uploaded or saved to a Google Cloud Storage bucket named coffee-sales-data.

This event triggers the etl_backdating_bigquery_cloud_run_function.py script deployed on Cloud Run.

The Cloud Run function reads the new data from the GCS bucket and loads it directly into a BigQuery table, making the new data available for near-real-time analysis.

4. The Role of LLMs
Many of the Python scripts in this repository were developed by writing comprehensive prompts for ChatGPT and Gemini. This approach allowed for rapid prototyping and script generation, showcasing how LLMs can be a powerful tool in a data engineer's workflow.

Dashboards and Visualizations
Both Power BI and Looker Studio dashboards were created to provide insightful visualizations of the sales data.

Power BI Dashboard: Connects to the MySQL and PostgreSQL databases to visualize on-premise data, focusing on transaction-level or aggregated metrics.

Looker Studio Dashboard: Connects to the BigQuery data warehouse, leveraging its speed and scale to display a dynamic and powerful dashboard for deeper analytics.

Future Enhancements
Implement a robust error handling and logging system for all ETL scripts.

Set up a data quality monitoring framework to ensure data accuracy and consistency at each stage of the pipeline.

