

SELECT *
FROM "my_etl_db"."main"."stg_orders"
WHERE amount <= 0
  AND amount IS NOT NULL

