

SELECT *
FROM "my_etl_db"."main_analytics"."fct_sales"
WHERE amount <= 0
  AND amount IS NOT NULL

