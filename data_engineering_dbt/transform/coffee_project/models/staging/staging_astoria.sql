{{ config(
    materialized='view',
    alias='staging_astoria'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ source('raw', 'raw_astoria') }}

),

deduplicated AS (

    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY ingestion_timestamp DESC
        ) AS row_num

    FROM source_data

)

SELECT
    transaction_id,
    PARSE_DATE('%Y/%m/%d', transaction_date) AS transaction_date,
    transaction_time,
    transaction_qty,
    store_id,
    store_location,
    "West Campus" AS converted_store_location,
    product_id,
    unit_price,
    unit_price * 15 AS unit_price_rand,
    product_category,
    product_type,
    product_detail,
    ingestion_timestamp

FROM deduplicated
WHERE row_num = 1
