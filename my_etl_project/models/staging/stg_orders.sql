-- models/staging/stg_orders.sql
-- Materialization: view (inherited from dbt_project.yml staging config)
-- Purpose: Clean & standardize raw orders from the source layer

SELECT
    order_id,
    customer_id,
    UPPER(TRIM(status))             AS status,        -- Normalize: 'complete' → 'COMPLETE'
    CAST(amount AS DECIMAL)         AS amount,        -- Ensure numeric type
    CAST(order_date AS DATE)        AS order_date,    -- Ensure date type
    country_code,
    CAST(updated_at AS TIMESTAMP)   AS updated_at

FROM {{ source('raw', 'raw_orders') }}   -- References src_raw.yml → raw_ingestion.raw_orders

WHERE order_id IS NOT NULL               -- Basic data quality filter
