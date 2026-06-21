{{ config(
    materialized='view',
    alias='int_sales'
) }}

SELECT *,
    EXTRACT(YEAR FROM transaction_date) AS year,
    EXTRACT(MONTH FROM transaction_date) AS month,
    EXTRACT(DAY FROM transaction_date) AS day,
    EXTRACT(DAYOFWEEK FROM transaction_date) AS day_of_week

FROM {{ ref('int_combined_stores') }}
