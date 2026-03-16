
  
    

  create  table "my_etl_db"."main_analytics"."fct_sales__dbt_tmp"
  
  
    as
  
  (
    -- models/marts/fct_sales.sql
-- Materialization: TABLE in the 'analytics' schema (from dbt_project.yml)
-- Purpose: Final sales fact table for BI tools and dashboards.
-- Tagged 'daily' and 'sales' for selective scheduling.



SELECT
    o.order_id,
    o.customer_id,
    o.customer_name,
    o.region,
    o.segment,
    o.status,
    o.amount,
    
    (o.amount / 100.0)
   AS amount_dollars, -- macro usage
    o.order_date,
    o.country_code,
    cc.country_name,
    cc.region                            AS country_region,
    o.updated_at,
    CURRENT_TIMESTAMP                    AS dbt_loaded_at   -- audit column

FROM "my_etl_db"."main"."int_orders_customers"   AS o

LEFT JOIN "my_etl_db"."main_raw_ingestion"."country_codes"     AS cc
    ON o.country_code = cc.country_code

WHERE o.status = 'COMPLETE'    -- Fact table: only completed orders
  );
  