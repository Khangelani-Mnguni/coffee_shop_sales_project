{{ config(
    materialized='table',
    alias='mart_bi'
) }}

SELECT
    transaction_date,
    year,
    month,
    day,
    day_of_week,
    store_location,
    product_category,
    product_type,
    product_detail,
    transaction_qty,
    total_revenue

FROM {{ ref('int_sales') }}