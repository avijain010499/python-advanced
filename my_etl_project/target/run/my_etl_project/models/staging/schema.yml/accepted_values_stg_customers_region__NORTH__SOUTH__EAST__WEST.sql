
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        region as value_field,
        count(*) as n_records

    from "my_etl_db"."main"."stg_customers"
    group by region

)

select *
from all_values
where value_field not in (
    'NORTH','SOUTH','EAST','WEST'
)



  
  
      
    ) dbt_internal_test