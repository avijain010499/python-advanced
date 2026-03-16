
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select region
from "my_etl_db"."main"."stg_customers"
where region is null



  
  
      
    ) dbt_internal_test