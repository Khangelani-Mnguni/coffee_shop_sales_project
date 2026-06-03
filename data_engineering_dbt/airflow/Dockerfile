FROM apache/airflow:2.9.3

# Switch to root to install any system dependencies (optional but good for dbt)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && apt-get clean

# Switch back to airflow user to install Python packages
USER airflow

# Pre-install your requirements
RUN pip install --no-cache-dir \
    "dbt-bigquery" \
    "google-cloud-bigquery" \
    "protobuf<5.0.0"