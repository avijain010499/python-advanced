-- models/intermediate/int_orders_customers.sql
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

FROM {{ ref('int_orders_cleaned') }}  AS o   -- Ephemeral: inlines as CTE
JOIN {{ ref('stg_customers') }}       AS c
    ON o.customer_id = c.customer_id
