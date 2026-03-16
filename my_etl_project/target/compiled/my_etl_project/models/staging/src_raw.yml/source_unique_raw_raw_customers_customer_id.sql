
    
    

select
    customer_id as unique_field,
    count(*) as n_records

from "my_etl_db"."main_raw_ingestion"."raw_customers"
where customer_id is not null
group by customer_id
having count(*) > 1


