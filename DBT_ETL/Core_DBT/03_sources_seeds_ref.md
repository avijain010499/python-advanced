# 📋 Sources, Seeds & ref() in dbt

---

## 🤔 What Are Sources and Seeds?

**Sources** = raw tables already loaded into your warehouse (by Airflow, Fivetran, etc.)  
**Seeds** = CSV files you put in your dbt project that dbt loads as tables  
**ref()** = how dbt models reference each other (builds the dependency graph)

---

## 💻 Example 1: Defining Sources in `schema.yml`

```python
# File: models/staging/schema.yml
sources_yaml = """
version: 2

sources:
  - name: raw                      # Logical name for this source group
    database: RAW_DB               # Actual database in warehouse (optional)
    schema: raw_ingestion           # Actual schema where raw tables live

    tables:
      - name: orders               # Actual table name: raw_ingestion.orders
        description: "Raw orders from the transactional database"
        loaded_at_field: _extracted_at   # Column dbt uses for freshness check
        freshness:
          warn_after:  {count: 6,  period: hour}   # Alert if data > 6h old
          error_after: {count: 24, period: hour}   # Error if data > 24h old
        columns:
          - name: order_id
            description: "Primary key"
            tests:
              - not_null
              - unique

      - name: customers
        description: "Raw customer records"
        columns:
          - name: customer_id
            tests: [not_null, unique]

      - name: products
        description: "Product catalog"
"""
print(sources_yaml)

# Check source freshness (run in terminal):
# !dbt source freshness
print("\nRun: dbt source freshness   → checks if source data is stale")
```

---

## 💻 Example 2: Using `source()` in SQL Models

```python
# In models/staging/stg_orders.sql:
stg_orders_sql = """
SELECT
    order_id,
    customer_id,
    UPPER(TRIM(status))     AS status,
    CAST(amount AS DECIMAL) AS amount,
    CAST(order_date AS DATE) AS order_date

FROM {{ source('raw', 'orders') }}
--       ↑ source group name   ↑ table name from schema.yml
--
-- dbt compiles this to:
-- FROM raw_ingestion.orders
-- (actual schema from schema.yml definition)

WHERE order_id IS NOT NULL
"""
print("Use source() for raw tables, ref() for dbt models")
print(stg_orders_sql)
```

---

## 💻 Example 3: Using `ref()` — The Model Dependency Graph

```python
# ref() creates a dependency — dbt builds models in the right order!

ref_example = """
-- models/intermediate/int_orders_customers.sql
-- dbt knows to run stg_orders and stg_customers BEFORE this model
SELECT
    o.order_id,
    o.amount,
    o.order_date,
    c.customer_name,
    c.region,
    c.segment

FROM {{ ref('stg_orders') }}    AS o   -- dbt model ref
JOIN {{ ref('stg_customers') }} AS c   ON o.customer_id = c.customer_id

-- dbt compiles ref('stg_orders') to the actual table/view name:
-- dev:   dev_myschema.stg_orders
-- prod:  prod_analytics.stg_orders
-- (environment prefix added automatically based on profile target)
"""
print(ref_example)

dependency_order = """
Dependency graph dbt builds automatically:
  source('raw','orders')     → stg_orders
  source('raw','customers') → stg_customers
  stg_orders + stg_customers → int_orders_customers
  int_orders_customers       → fct_sales

dbt runs in this exact order: sources → staging → intermediate → marts
"""
print(dependency_order)
```

---

## 💻 Example 4: Seeds — Load CSVs as Tables

```python
# Seeds are CSV files in the seeds/ folder that dbt loads as tables
# Great for: lookup tables, country codes, exchange rates, mapping tables

# File: seeds/country_codes.csv content:
seed_csv = """country_code,country_name,region
US,United States,North America
UK,United Kingdom,Europe
IN,India,Asia
DE,Germany,Europe
BR,Brazil,South America"""

print("File: seeds/country_codes.csv")
print(seed_csv)

# Load seeds to warehouse:
print("\nRun: dbt seed")
print("Creates table: dev_myschema.country_codes in your warehouse")

# Use in a model:
use_seed = """
-- models/marts/fct_sales_with_country.sql
SELECT
    s.*,
    c.country_name,
    c.region

FROM {{ ref('fct_sales') }}       AS s
LEFT JOIN {{ ref('country_codes') }} AS c   -- ref() works for seeds too!
    ON s.country_code = c.country_code
"""
print("\nUsing a seed in a model:")
print(use_seed)
```

---

## 💻 Example 5: Seeds Configuration in `dbt_project.yml`

```python
seeds_config = """
# In dbt_project.yml:
seeds:
  my_etl_project:
    +schema: reference_data     # Load seeds into 'reference_data' schema
    country_codes:
      +column_types:
        country_code: varchar(5)   # Override inferred type
    exchange_rates:
      +enabled: true
"""
print(seeds_config)

# Run only specific seed:
print("dbt seed --select country_codes")

# Run seed and downstream models:
print("dbt seed --select country_codes && dbt run --select fct_sales_with_country")
```

---

## 🏭 Summary

| Feature          | Purpose                             | Syntax                              |
| ---------------- | ----------------------------------- | ----------------------------------- |
| `source()`       | Reference raw warehouse tables      | `{{ source('group', 'table') }}`    |
| `ref()`          | Reference another dbt model or seed | `{{ ref('model_name') }}`           |
| Seeds            | Load CSV into warehouse as a table  | Put CSV in `seeds/`, run `dbt seed` |
| Source freshness | Alert on stale source data          | `dbt source freshness`              |

---

## ⚠️ Common Mistakes

```python
mistakes = [
    ("Hardcoding schema.table in SQL", "Use source() or ref() — paths change per environment!"),
    ("Putting seeds > 1MB in repo",    "Seeds are for small lookup tables, not bulk data"),
    ("Forgetting freshness config",    "No freshness = no alert when source data stops arriving"),
]
for mistake, fix in mistakes:
    print(f"❌ {mistake}")
    print(f"✅ {fix}\n")
```
