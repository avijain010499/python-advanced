{{ config(
    materialized     = 'incremental',
    unique_key       = 'order_id',
    on_schema_change = 'sync_all_columns',
    schema           = 'analytics',
    tags             = ['daily', 'incremental']
) }}

SELECT
    order_id,
    customer_id,
    amount,
    status,
    order_date,
    country_code,
    updated_at,
    CURRENT_TIMESTAMP AS dbt_loaded_at

FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
  -- On incremental runs: only process rows newer than what's already in the table
  -- On first run: is_incremental() = False → loads ALL rows
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
