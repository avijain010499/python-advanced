-- models/staging/stg_products.sql
-- Materialization: view (inherited from dbt_project.yml staging config)
-- Purpose: Clean & standardize the product catalog

SELECT
    product_id,
    TRIM(product_name)                AS product_name,
    UPPER(TRIM(category))             AS category,
    CAST(price AS DECIMAL)            AS price_usd,
    UPPER(TRIM(currency))             AS currency

FROM {{ source('raw', 'raw_products') }}

WHERE product_id IS NOT NULL
