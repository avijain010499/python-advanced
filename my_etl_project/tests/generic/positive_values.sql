-- tests/generic/positive_values.sql
-- Generic reusable dbt test: checks that a column has only positive (> 0) values.
--
-- Usage in schema.yml:
--   tests:
--     - positive_values
--
-- If this SELECT returns ANY rows → test FAILS

{% test positive_values(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} <= 0
  AND {{ column_name }} IS NOT NULL

{% endtest %}
