

SELECT
    order_id,
    customer_id,
    amount,
    status,
    order_date,
    country_code,
    updated_at,
    CURRENT_TIMESTAMP AS dbt_loaded_at

FROM "my_etl_db"."main"."stg_orders"


  -- On incremental runs: only process rows newer than what's already in the table
  -- On first run: is_incremental() = False → loads ALL rows
  WHERE updated_at > (SELECT MAX(updated_at) FROM "my_etl_db"."main_analytics"."fct_sales_incremental")
