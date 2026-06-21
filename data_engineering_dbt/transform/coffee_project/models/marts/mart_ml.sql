{{ config(
    materialized='table',
    alias='mart_ml'
) }}

SELECT
    *
FROM {{ ref('int_sales') }}