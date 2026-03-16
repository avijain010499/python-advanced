-- models/intermediate/int_orders_cleaned.sql
-- Materialization: EPHEMERAL — becomes a CTE inside models that ref() this.
-- No table or view is created in the warehouse.
-- Purpose: Handle null/negative amounts and remove junk rows.



SELECT
    order_id,
    customer_id,
    CASE
        WHEN amount < 0     THEN 0      -- Negative amounts → treat as 0
        WHEN amount IS NULL THEN 0      -- Null amounts → treat as 0
        ELSE amount
    END                 AS amount_safe,
    status,
    order_date,
    country_code,
    updated_at

FROM "my_etl_db"."main"."stg_orders"

WHERE order_id IS NOT NULL