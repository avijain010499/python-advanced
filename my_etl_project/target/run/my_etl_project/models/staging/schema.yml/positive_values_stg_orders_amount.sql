
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

SELECT *
FROM "my_etl_db"."main"."stg_orders"
WHERE amount <= 0
  AND amount IS NOT NULL


  
  
      
    ) dbt_internal_test