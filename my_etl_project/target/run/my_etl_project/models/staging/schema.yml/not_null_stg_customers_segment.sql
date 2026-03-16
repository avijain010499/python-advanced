
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select segment
from "my_etl_db"."main"."stg_customers"
where segment is null



  
  
      
    ) dbt_internal_test