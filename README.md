# Coffee Shop Sales Analytics
Overview

This project demonstrates a complete end-to-end data pipeline and machine learning workflow for a coffee shop’s sales data. It combines data engineering to automate ingestion and transformation of transactional data with data science to build predictive models that uncover the key drivers of revenue.

The outcome is both robust infrastructure for ongoing analytics and business insights that guide pricing and sales strategy.

# Data Engineering

ETL Pipelines: Built scripts for historical backfilling and scheduled updates.

Sources & Destinations:

Raw CSV → MySQL (OLTP)

MySQL → PostgreSQL (OLAP)

Cloud integration with GCP (Cloud Storage, Cloud Run, BigQuery)

Orchestration: Automated with Apache Airflow (Dockerized, Linux/Ubuntu).

Dashboards:

Power BI (on-premise MySQL/Postgres data)

Looker Studio (BigQuery data warehouse).

LLM-Accelerated Development: Scripts were rapidly prototyped using ChatGPT and Gemini.

# Data Science

Data Preparation: Extracted Postgres views into pandas, cleaned and engineered features.

Models Used: Linear Regression, Decision Tree, Random Forest, XGBoost.

Evaluation: Feature importance analysis (focus on XGBoost).

Key Findings

Transaction Quantity is the most important driver of sales revenue.

Unit Price is the second key factor.

Year/Time Trends also influence demand.

Business Impact

To improve performance, focus on increasing items per transaction (bundles, loyalty rewards, seasonal promotions) and optimizing pricing (sensitivity testing, tiered pricing, off-peak discounts).

Tech Stack

Databases: MySQL, PostgreSQL, BigQuery

Languages/Libraries: Python (pandas, numpy, scikit-learn, xgboost), Matplotlib, Seaborn

Cloud: Google Cloud Platform (GCS, Cloud Run, BigQuery)

Orchestration: Apache Airflow (Dockerized)

BI Tools: Power BI, Looker Studio

AI Tools: ChatGPT, Gemini (LLM-assisted development)

Repository Structure
coffee_shop_sales_project/
├── data_engineering/   # ETL scripts, backfilling, scheduled jobs
├── data_science/       # Notebooks, ML models, feature importance
├── README.md           # Project documentation (this file)
└── requirements.txt    # Python dependencies

How to Run

Clone the repo:

git clone https://github.com/Khangelani-Mnguni/coffee_shop_sales_project.git
cd coffee_shop_sales_project


Install dependencies:

pip install -r requirements.txt


Explore data_engineering scripts (ETL pipelines).

Open data_science notebooks for analysis and ML models.

Author

Khangelani Mnguni
Business Intelligence Analyst | Aspiring Data Scientist
