# Coffee Shop Sales Analytics

## Overview
This project analyzes coffee shop sales data to uncover the key factors driving revenue performance. 
It combines **data engineering** and **data science** workflows, moving from raw transactional data stored in PostgreSQL to machine learning models that evaluate feature importance.

## Project Structure
```
coffee_shop_sales_project/
│
├── data_engineering/   # ETL processes and notebooks for pulling/cleaning data
├── data_science/       # Notebooks and scripts for feature engineering, ML models, and analysis
└── README.md           # Project documentation (this file)
```

## Data Engineering
- Data was extracted from a **PostgreSQL database**.
- Fact and dimension tables were combined into views suitable for analytics.
- Data was loaded into notebooks for cleaning and preprocessing with **pandas**.

## Data Science & Modeling
Several machine learning models were applied to understand the drivers of sales revenue:
- **Simple Linear Regression**
- **Decision Tree Regressor**
- **Random Forest Regressor**
- **XGBoost Regressor**

The models were compared using feature importance analysis to determine which factors most strongly influence revenue.

## Key Findings

### **Conclusion**
The feature importance analysis from the **XGBoost Regressor** shows that **transaction quantity** is by far the most influential factor in predicting sales revenue.  
This suggests that business performance is strongly tied to the **volume of products purchased per transaction**.  
The second most important driver is **unit price**, followed by **year (time trend effects)**.

From these results, it is clear that to improve performance, the business should focus on strategies that **increase the number of items customers buy in each transaction**, while also **optimizing pricing**.  
By balancing attractive pricing with initiatives that encourage higher transaction volumes, the coffee shop can **drive both sales growth and overall revenue performance more effectively**.

### **Recommendations**
#### 1. Increase Transaction Quantity
- Introduce **bundle deals** (e.g., “Buy 2 coffees, get 1 at half price”).  
- Launch **loyalty rewards** (e.g., free drink after 10 purchases).  
- Offer **seasonal promotions** (e.g., coffee + bakery item combos).  

#### 2. Optimize Pricing Strategy
- Conduct **price sensitivity analysis** to find the optimal price point.  
- Use **tiered pricing** (small, medium, large sizes).  
- Test **limited-time discounts** during off-peak hours.  

#### 3. Leverage Time Trends
- Monitor **seasonal demand patterns** (e.g., hot drinks in winter, iced drinks in summer).  
- Use **predictive analytics** to forecast demand.  
- Align **inventory and pricing** strategies with trends.  

By implementing these strategies, the coffee shop can boost both **transaction quantity** and **revenue**, while maintaining a pricing structure that attracts customers.

## Tech Stack
- **PostgreSQL** – Data storage and view creation from fact and dimension tables.  
- **Python (pandas, numpy)** – Data loading, cleaning, feature engineering.  
- **scikit-learn** – Machine learning models (Linear Regression, Decision Tree, Random Forest).  
- **XGBoost** – Gradient boosting model for feature importance analysis.  
- **Matplotlib & Seaborn** – Data visualization and plotting.  
- **Jupyter Notebook** – Interactive data analysis environment.  

## How to Run
1. Clone this repository:  
   ```bash
   git clone https://github.com/Khangelani-Mnguni/coffee_shop_sales_project.git
   cd coffee_shop_sales_project
   ```

2. Install dependencies:  
   ```bash
   pip install -r requirements.txt
   ```

3. Open the Jupyter notebooks to explore data engineering and data science workflows:  
   ```bash
   jupyter notebook
   ```

## Author
**Khangelani Mnguni**  
BI Analyst & Aspiring Data Scientist  
