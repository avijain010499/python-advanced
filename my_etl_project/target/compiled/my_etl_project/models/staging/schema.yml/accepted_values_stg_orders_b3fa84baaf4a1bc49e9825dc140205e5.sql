
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from "my_etl_db"."main"."stg_orders"
    group by status

)

select *
from all_values
where value_field not in (
    'COMPLETE','PENDING','FAILED','CANCELLED'
)


