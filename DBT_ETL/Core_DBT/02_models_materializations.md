# 🧱 dbt Models & Materializations

---

## 🤔 What Is a dbt Model?

A **dbt model** is just a `.sql` file containing a `SELECT` statement. dbt wraps it in a `CREATE TABLE AS` or `CREATE VIEW AS` automatically — you never write DDL.

> 💡 **Key insight**: You write `SELECT`, dbt handles all the `CREATE`, `DROP`, `INSERT` plumbing. This keeps your SQL clean and readable.

---

## 🧱 The 4 Materializations

| Type          | What dbt Creates                    | Best For                        |
| ------------- | ----------------------------------- | ------------------------------- |
| `view`        | A SQL view (no data copy)           | Staging, lightweight transforms |
| `table`       | A full physical table (data stored) | Final marts, expensive queries  |
| `incremental` | Append/update only new rows         | Facts with millions of rows     |
| `ephemeral`   | CTE — exists only in memory         | Intermediate reusable logic     |

---

## 💻 Example 1: Your First Model — Staging

```python
# File: models/staging/stg_orders.sql
stg_orders_sql = """
-- This model cleans raw orders from the source system
-- dbt will CREATE OR REPLACE VIEW stg_orders AS (this SELECT)

SELECT
    order_id,
    customer_id,
    UPPER(TRIM(status))                          AS status,
    CAST(amount AS DECIMAL(18,2))                AS amount,
    CAST(order_date AS DATE)                     AS order_date,
    UPPER(TRIM(region))                          AS region,
    CURRENT_TIMESTAMP()                          AS dbt_loaded_at

FROM {{ source('raw', 'orders') }}   -- References your source definition

WHERE order_id IS NOT NULL
"""
print("Model: models/staging/stg_orders.sql")
print(stg_orders_sql)
```

---

## 💻 Example 2: Materialized as Table — Final Mart

```python
# File: models/marts/fct_sales.sql
fct_sales_sql = """
{{
  config(
    materialized = 'table',
    schema       = 'analytics',
    tags         = ['daily', 'sales']
  )
}}

-- Final fact table — joins staging models together
SELECT
    o.order_id,
    o.order_date,
    o.status,
    o.amount,
    c.customer_name,
    c.segment,
    c.region,
    p.product_name,
    p.category

FROM {{ ref('stg_orders') }}    AS o       -- ref() = reference another dbt model
LEFT JOIN {{ ref('stg_customers') }} AS c  ON o.customer_id = c.customer_id
LEFT JOIN {{ ref('stg_products') }}  AS p  ON o.product_id  = p.product_id

WHERE o.status = 'COMPLETE'
"""
print("Model: models/marts/fct_sales.sql")
print(fct_sales_sql)
```

---

## 💻 Example 3: Incremental Model — ETL Performance

```python
# File: models/marts/fct_sales_incremental.sql
incremental_sql = """
{{
  config(
    materialized  = 'incremental',
    unique_key    = 'order_id',        -- Used to MERGE/UPDATE existing rows
    on_schema_change = 'sync_all_columns'
  )
}}

SELECT
    order_id,
    customer_id,
    amount,
    order_date,
    status,
    updated_at

FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
  -- On incremental runs: only process rows newer than the last load
  -- On first run: is_incremental() = False → loads everything
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
"""
print("Incremental model — only processes NEW rows on subsequent runs!")
print(incremental_sql)

explanation = """
How incremental materializations work:
1. First run:  dbt runs full SELECT → creates table (all rows)
2. Second run: is_incremental() = True → WHERE clause filters to new rows only
3. dbt MERGEs the new rows into the existing table (updates or inserts)

Benefits:
- 10-100x faster than rebuilding the full table each time
- Ideal for fact tables with millions of rows
"""
print(explanation)
```

---

## 💻 Example 4: Ephemeral Model — Reusable CTEs

```python
# File: models/intermediate/int_orders_cleaned.sql
ephemeral_sql = """
{{
  config(materialized = 'ephemeral')
}}

-- Ephemeral: becomes a CTE inside whatever model ref()s this
-- No table or view is created in the warehouse
SELECT
    order_id,
    customer_id,
    CASE
        WHEN amount < 0    THEN 0
        WHEN amount IS NULL THEN 0
        ELSE amount
    END AS amount_safe,
    order_date

FROM {{ source('raw', 'orders') }}
WHERE order_id IS NOT NULL
"""
print("Ephemeral model: becomes a CTE in any model that ref()s it")
print(ephemeral_sql)
```

---

## 💻 Example 5: Model Layers — Staging → Intermediate → Marts

```python
# Standard dbt project layer pattern:

layers = {
    "staging/ (views)": [
        "One model per source table",
        "Rename columns, cast types, clean strings",
        "Never join or aggregate here",
        "e.g., stg_orders.sql, stg_customers.sql",
    ],
    "intermediate/ (views/ephemeral)": [
        "Join staging models together",
        "Apply business logic",
        "Not exposed to BI tools",
        "e.g., int_orders_with_customers.sql",
    ],
    "marts/ (tables)": [
        "Final, analytics-ready tables and views",
        "Exposed to BI tools (Tableau, Looker)",
        "Named by business domain: fct_, dim_",
        "e.g., fct_sales.sql, dim_customer.sql",
    ],
}

for layer, points in layers.items():
    print(f"\n📁 models/{layer}")
    for p in points:
        print(f"   • {p}")
```

---

## ⚠️ Common Mistakes

```python
mistakes = {
    "Writing CREATE TABLE in a model": "Just write SELECT — dbt handles DDL automatically",
    "Joining in staging models": "Staging = one source table, one model. Join in intermediate/",
    "Hardcoding table names": "Use ref() for models and source() for raw tables",
    "Not setting materialization": "Default is view — set tables for expensive marts",
}
for mistake, fix in mistakes.items():
    print(f"❌ {mistake}")
    print(f"✅ Fix: {fix}\n")
```
