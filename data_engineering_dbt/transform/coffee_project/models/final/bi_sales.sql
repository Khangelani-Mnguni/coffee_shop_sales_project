SELECT
    transaction_date,
    store_location,
    product_category,
    total_quantity,
    total_revenue

FROM {{ ref('sales_mart') }}