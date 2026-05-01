SELECT
    transaction_date,
    product_category,
    store_location,
    total_quantity,
    total_revenue

FROM {{ ref('sales_mart') }}