
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        segment as value_field,
        count(*) as n_records

    from "my_etl_db"."main"."stg_customers"
    group by segment

)

select *
from all_values
where value_field not in (
    'PLATINUM','GOLD','SILVER'
)



  
  
      
    ) dbt_internal_test