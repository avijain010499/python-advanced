
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_id
from "my_etl_db"."main"."int_orders_customers"
where order_id is null



  
  
      
    ) dbt_internal_test