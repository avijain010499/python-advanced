with __dbt__cte__int_orders_cleaned as (
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
) -- models/intermediate/int_orders_customers.sql
-- Materialization: view (inherited from dbt_project.yml intermediate config)
-- Purpose: Enrich orders with customer dimension data.
-- dbt knows to run stg_orders and stg_customers BEFORE this model (via ref()).

SELECT
    o.order_id,
    o.amount_safe                   AS amount,
    o.status,
    o.order_date,
    o.country_code,
    o.updated_at,
    c.customer_id,
    c.customer_name,
    c.region,
    c.segment

FROM __dbt__cte__int_orders_cleaned  AS o   -- Ephemeral: inlines as CTE
JOIN "my_etl_db"."main"."stg_customers"       AS c
    ON o.customer_id = c.customer_id