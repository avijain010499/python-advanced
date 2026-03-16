
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price_usd
from "my_etl_db"."main"."stg_products"
where price_usd is null



  
  
      
    ) dbt_internal_test