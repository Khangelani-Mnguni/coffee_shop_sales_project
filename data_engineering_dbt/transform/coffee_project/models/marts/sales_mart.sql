SELECT
    transaction_date,
    store_location,
    product_category,
    SUM(transaction_qty) AS total_quantity,
    SUM(transaction_qty * unit_price) AS total_revenue

FROM {{ ref('staging_lower_manhattan') }}

GROUP BY 1,2,3