
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_id
from "my_etl_db"."main_raw_ingestion"."raw_products"
where product_id is null



  
  
      
    ) dbt_internal_test