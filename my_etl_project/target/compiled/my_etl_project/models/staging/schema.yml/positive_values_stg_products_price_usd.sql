

SELECT *
FROM "my_etl_db"."main"."stg_products"
WHERE price_usd <= 0
  AND price_usd IS NOT NULL

