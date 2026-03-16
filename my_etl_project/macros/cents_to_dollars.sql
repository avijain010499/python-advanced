-- macros/cents_to_dollars.sql
-- Reusable Jinja macro: divides a column value by 100 to convert cents to dollars.
--
-- Usage in SQL models:
--   {{ cents_to_dollars('amount_column') }}
--
-- Example output: (amount_column / 100.0)

{% macro cents_to_dollars(column_name) %}
    ({{ column_name }} / 100.0)
{% endmacro %}
