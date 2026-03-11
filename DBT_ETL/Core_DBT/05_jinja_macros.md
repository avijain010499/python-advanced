# 🧩 Jinja & Macros in dbt

---

## 🤔 What Is Jinja in dbt?

dbt uses **Jinja templating** inside your SQL files. Jinja lets you write Python-like logic — variables, conditionals, loops — directly in SQL. This makes your SQL DRY (Don't Repeat Yourself) and dynamic.

> 💡 **Key insight**: When dbt compiles your model, it runs Jinja first, producing plain SQL, which then runs on your warehouse. You never see Jinja — your warehouse only sees regular SQL.

---

## 💻 Example 1: Jinja Basics in SQL

```python
jinja_basics = """
-- Variables
{% set revenue_threshold = 1000 %}
{% set status_list = ['COMPLETE', 'SHIPPED'] %}

SELECT
    order_id,
    amount,
    CASE WHEN amount > {{ revenue_threshold }} THEN 'High' ELSE 'Low' END AS tier

FROM {{ ref('stg_orders') }}

WHERE status IN (
    {% for s in status_list %}
        '{{ s }}'{% if not loop.last %},{% endif %}
    {% endfor %}
)
-- Compiled SQL:
-- WHERE status IN ('COMPLETE', 'SHIPPED')
"""
print(jinja_basics)

# if/else in Jinja
conditional = """
SELECT
    order_id,
    {% if target.name == 'prod' %}
        amount                    -- In prod: use real amounts
    {% else %}
        ROUND(amount, -2)         -- In dev: round to nearest 100 for privacy
    {% endif %} AS amount

FROM {{ ref('stg_orders') }}
"""
print("Conditional based on environment:")
print(conditional)
```

---

## 💻 Example 2: Macros — Reusable SQL Functions

```python
# File: macros/cents_to_dollars.sql
macro_cents_to_dollars = """
{% macro cents_to_dollars(column_name, precision=2) %}
    ROUND({{ column_name }} / 100.0, {{ precision }})
{% endmacro %}
"""
print(macro_cents_to_dollars)

# Use in any model:
use_macro = """
SELECT
    order_id,
    {{ cents_to_dollars('amount_cents') }}      AS amount_usd,
    {{ cents_to_dollars('shipping_cents', 0) }} AS shipping_usd
FROM {{ ref('stg_orders') }}
-- Compiled to:
-- ROUND(amount_cents / 100.0, 2) AS amount_usd
"""
print(use_macro)
```

---

## 💻 Example 3: Useful Built-In dbt Macros

```python
builtin_macros = """
-- 1. run_query() — execute a query and use the results in Jinja
{% set regions = run_query("SELECT DISTINCT region FROM " ~ ref('stg_orders')) %}
{% set region_list = regions.columns[0].values() %}

-- 2. log() — print to the console during dbt run (for debugging)
{% set row_count = run_query("SELECT COUNT(*) FROM " ~ ref('stg_orders')) %}
{{ log("Row count: " ~ row_count.columns[0].values()[0], info=True) }}

-- 3. this — refers to the current model (used in incremental models)
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})

-- 4. target — information about the current environment
{{ target.name }}     -- 'dev' or 'prod'
{{ target.schema }}   -- 'dev_myschema' or 'analytics'
{{ target.type }}     -- 'snowflake', 'bigquery', 'postgres'

-- 5. env_var() — read environment variables
{% set api_key = env_var('MY_API_KEY') %}
"""
print(builtin_macros)
```

---

## 💻 Example 4: Macro — Generate Surrogate Key

```python
# File: macros/generate_surrogate_key.sql
# (dbt_utils package has this — shown here for teaching purposes)
surrogate_key_macro = """
{% macro generate_surrogate_key(field_list) %}
    MD5(
        CONCAT_WS('||',
            {% for field in field_list %}
                CAST({{ field }} AS STRING){% if not loop.last %}, {% endif %}
            {% endfor %}
        )
    )
{% endmacro %}
"""
print(surrogate_key_macro)

usage = """
-- In a model: generate a stable surrogate key from natural keys
SELECT
    {{ generate_surrogate_key(['order_id', 'line_item_id']) }} AS order_line_key,
    order_id,
    line_item_id,
    product_id,
    quantity
FROM {{ ref('stg_order_items') }}
"""
print(usage)
```

---

## 💻 Example 5: dbt Packages — Pre-Built Macros

```python
# File: packages.yml — add popular open-source dbt packages
packages_yml = """
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
    # Provides: surrogate_key, date_spine, union_relations, pivot, etc.

  - package: calogica/dbt_date
    version: [">=0.9.0", "<1.0.0"]
    # Provides: date spine, fiscal calendar utilities
"""
print(packages_yml)
print("\nInstall packages:  dbt deps")

# Popular macros from dbt_utils:
dbt_utils_examples = """
-- Generate surrogate key
{{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }}

-- Safe divide (avoids division by zero)
{{ dbt_utils.safe_divide('revenue', 'cost') }}

-- Date spine (generate sequence of dates)
{{ dbt_utils.date_spine(
    datepart = 'day',
    start_date = "'2024-01-01'",
    end_date   = "current_date"
) }}

-- Star: select all except a few columns
{{ dbt_utils.star(from=ref('stg_orders'), except=['_loaded_at','_raw_id']) }}
"""
print(dbt_utils_examples)
```

---

## 🏭 Summary

| Feature                | Use Case                               |
| ---------------------- | -------------------------------------- |
| `{% set x = ... %}`    | Define Jinja variables in SQL          |
| `{{ variable }}`       | Render a variable into SQL             |
| `{% if target.name %}` | Different logic per environment        |
| `{% macro name() %}`   | Reusable SQL snippet (like a function) |
| `{{ this }}`           | Reference the current model's table    |
| `dbt deps`             | Install packages.yml packages          |

---

## ⚠️ Common Mistakes

```python
mistakes = [
    ("Forgetting dbt deps after adding packages.yml", "Always run 'dbt deps' first!"),
    ("Putting logic in Jinja that should be in SQL", "Keep SQL readable — Jinja for structure, SQL for data logic"),
    ("Using run_query() carelessly", "run_query runs at compile time, not run time — can be slow on large tables"),
]
for mistake, fix in mistakes:
    print(f"❌ {mistake}")
    print(f"✅ {fix}\n")
```
