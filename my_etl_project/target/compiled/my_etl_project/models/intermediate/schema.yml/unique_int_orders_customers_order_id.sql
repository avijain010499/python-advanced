
    
    

select
    order_id as unique_field,
    count(*) as n_records

from "my_etl_db"."main"."int_orders_customers"
where order_id is not null
group by order_id
having count(*) > 1


