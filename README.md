# Coffee Shop Sales Analytics

## Overview
This project demonstrates a complete end-to-end data pipeline and machine learning workflow for a coffee shop’s sales data. It combines data engineering to automate the ingestion and transformation of transactional data with data science to build predictive models that uncover the key drivers of revenue.

The outcome is a robust infrastructure for ongoing analytics and actionable business insights that guide pricing and sales strategy.

---

## Data Engineering

* **ETL Pipelines:** Built scripts for historical backfilling and scheduled updates.
* **Sources & Destinations:**
  * Raw CSV → MySQL (OLTP)
  * MySQL → PostgreSQL (OLAP)
  * Cloud integration with Google Cloud Platform (Cloud Storage, Cloud Run, BigQuery)
* **Orchestration:** Automated with Apache Airflow (Dockerized, Linux/Ubuntu).
* **Dashboards:**
  * **Power BI:** Sourced from on-premise MySQL/Postgres data.
  * **Looker Studio:** Sourced from BigQuery data warehouse.
* **LLM-Accelerated Development:** Scripts were rapidly prototyped using ChatGPT and Gemini.

---

## Data Analytics

* **Data Preparation:** Extracted Postgres views into `pandas`, then cleaned and engineered features.
* **Models Used:** Linear Regression, Decision Tree, Random Forest, XGBoost.
* **Evaluation:** Feature importance analysis (with a focus on XGBoost).

### Key Findings
1. **Transaction Quantity** is the most important driver of sales revenue.
2. **Unit Price** is the second key factor.
3. **Year/Time Trends** also significantly influence demand.

### Business Impact
To improve performance, business strategies should focus on:
* **Increasing items per transaction:** Implement product bundles, loyalty rewards, and seasonal promotions.
* **Optimizing pricing:** Utilize price sensitivity testing, tiered pricing, and off-peak discounts.

---

## Tech Stack

| Category | Technologies |
| :--- | :--- |
| **Databases** | MySQL, PostgreSQL, BigQuery |
| **Transformation Tools** | dbt |
| **Languages & Libraries** | Python (`pandas`, `numpy`, `scikit-learn`, `xgboost`), Matplotlib, Seaborn |
| **Cloud** | Google Cloud Platform (GCS, Cloud Run, BigQuery) |
| **Orchestration** | Apache Airflow (Dockerized) |
| **BI Tools** | Power BI, Looker Studio |

---

## Repository Structure

```text
coffee_shop_sales_project/
├── data_engineering_custom_code/   # ETL scripts, backfilling, scheduled jobs
├── data_engineering_dbt/           # raw_tables, terraform, dbt models, scheduled jobs
├── data_science/                   # Notebooks, ML models, feature importance
└── requirements.txt                # Python dependencies
```

---

## How to Run

1. **Clone the repository:**
```bash
   git clone [https://github.com/Khangelani-Mnguni/coffee_shop_sales_project.git](https://github.com/Khangelani-Mnguni/coffee_shop_sales_project.git)
   cd coffee_shop_sales_project
   ```

2. **Install dependencies:**
```bash
   pip install -r requirements.txt
   ```

3. **Explore the project:**
   * Navigate to the `data_engineering_*` directories to view the ETL/ELT pipelines and scheduled jobs.
   * Open the `data_science` notebooks to view the exploratory data analysis and machine learning models.

---

## Author

**Khangelani Mnguni**  
*Business Intelligence Analyst | Aspiring Data Scientist*
