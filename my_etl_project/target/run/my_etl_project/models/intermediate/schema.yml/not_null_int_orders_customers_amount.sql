
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select amount
from "my_etl_db"."main"."int_orders_customers"
where amount is null



  
  
      
    ) dbt_internal_test