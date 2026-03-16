
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

SELECT *
FROM "my_etl_db"."main"."stg_products"
WHERE price_usd <= 0
  AND price_usd IS NOT NULL


  
  
      
    ) dbt_internal_test