{{ config(
    materialized='view',
    alias='int_combined_stores'
) }}

WITH store_data AS (
    SELECT 
        transaction_id,
        transaction_date,
        converted_store_location AS store_location,
        product_category,
        product_detail,
        product_type,
        transaction_qty,
        unit_price_rand AS unit_price,
        unit_price_rand * transaction_qty AS total_revenue
    FROM {{ ref('staging_lower_manhattan') }}

    UNION ALL

    SELECT 
        transaction_id,
        transaction_date,
        converted_store_location AS store_location,
        product_category,
        product_detail,
        product_type,
        transaction_qty,
        unit_price_rand AS unit_price,
        unit_price_rand * transaction_qty AS total_revenue
    FROM {{ ref('staging_hells_kitchen') }}

    UNION ALL

    SELECT 
        transaction_id,
        transaction_date,
        converted_store_location AS store_location,
        product_category,
        product_detail,
        product_type,
        transaction_qty,
        unit_price_rand AS unit_price,
        unit_price_rand * transaction_qty AS total_revenue
    FROM {{ ref('staging_astoria') }}
)

SELECT * FROM store_data