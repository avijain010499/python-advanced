
  create view "my_etl_db"."main"."stg_customers__dbt_tmp"
    
    
  as (
    -- models/staging/stg_customers.sql
-- Materialization: view (inherited from dbt_project.yml staging config)
-- Purpose: Clean & standardize raw customer records

SELECT
    customer_id,
    TRIM(customer_name)  AS customer_name,
    UPPER(TRIM(region))  AS region,         -- Normalize region values
    UPPER(TRIM(segment)) AS segment,        -- Normalize: 'gold' → 'GOLD'
    LOWER(TRIM(email))   AS email,          -- Standardize email to lowercase
    country_code

FROM "my_etl_db"."main_raw_ingestion"."raw_customers"

WHERE customer_id IS NOT NULL
  );