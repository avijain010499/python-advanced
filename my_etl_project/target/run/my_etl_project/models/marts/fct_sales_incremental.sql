
      
        
        
        delete from "my_etl_db"."main_analytics"."fct_sales_incremental" as DBT_INTERNAL_DEST
        where (order_id) in (
            select distinct order_id
            from "fct_sales_incremental__dbt_tmp085640091741" as DBT_INTERNAL_SOURCE
        );

    

    insert into "my_etl_db"."main_analytics"."fct_sales_incremental" ("order_id", "customer_id", "amount", "status", "order_date", "country_code", "updated_at", "dbt_loaded_at")
    (
        select "order_id", "customer_id", "amount", "status", "order_date", "country_code", "updated_at", "dbt_loaded_at"
        from "fct_sales_incremental__dbt_tmp085640091741"
    )
  