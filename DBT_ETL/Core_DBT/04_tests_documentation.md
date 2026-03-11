# ✅ dbt Tests & Documentation

---

## 🤔 Why Are Tests Critical?

Bad data silently corrupts business decisions. dbt tests run automatically after every model build and catch issues like:

- Duplicate primary keys
- Null values in required columns
- Values outside an expected set
- Broken referential integrity between tables

---

## 💻 Example 1: Built-In Schema Tests

```python
# File: models/staging/schema.yml
schema_tests = """
version: 2

models:
  - name: stg_orders
    description: "Cleaned and standardized orders from the raw layer"
    columns:
      - name: order_id
        description: "Primary key — must be unique and not null"
        tests:
          - not_null             # Fails if any row has null order_id
          - unique               # Fails if any order_id appears more than once

      - name: status
        description: "Order status"
        tests:
          - not_null
          - accepted_values:
              values: ['COMPLETE', 'PENDING', 'FAILED', 'CANCELLED']
              # Fails if any status value is not in this list!

      - name: customer_id
        description: "Foreign key to stg_customers"
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
              # Fails if any customer_id has no match in stg_customers!

      - name: amount
        description: "Order amount in USD"
        tests:
          - not_null
"""
print(schema_tests)
print("Run:  dbt test --select stg_orders")
print("Run:  dbt test               ← run ALL tests across all models")
```

---

## 💻 Example 2: Custom Generic Tests

```python
# File: tests/generic/positive_values.sql
# Create your own reusable test

positive_values_sql = """
-- Generic test: check that a column has only positive values
-- Usage in schema.yml:  - positive_values

{% test positive_values(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} <= 0
  AND {{ column_name }} IS NOT NULL
{% endtest %}
-- If this SELECT returns ANY rows → test FAILS
"""
print(positive_values_sql)

# Then use in schema.yml:
usage = """
columns:
  - name: amount
    tests:
      - positive_values   # Uses your custom test above!
"""
print(usage)
```

---

## 💻 Example 3: Singular (One-Off) Tests

```python
# File: tests/assert_revenue_matches_invoices.sql
# Singular test: a one-off SQL query — if it returns rows, it FAILS

singular_test_sql = """
-- This test checks that the total revenue in fct_sales
-- matches the total in the invoices source table.
-- Data pipeline integrity check!

SELECT
    'Revenue mismatch' AS issue,
    ABS(fct.total - raw.total) AS difference_usd

FROM (SELECT SUM(amount) AS total FROM {{ ref('fct_sales') }}) AS fct
CROSS JOIN (SELECT SUM(invoice_amount) AS total FROM {{ source('raw','invoices') }}) AS raw

WHERE ABS(fct.total - raw.total) > 0.01   -- Allow $0.01 tolerance for rounding
"""
print("File: tests/assert_revenue_matches_invoices.sql")
print(singular_test_sql)
print("\nRun: dbt test --select assert_revenue_matches_invoices")
```

---

## 💻 Example 4: Test Severity — Warn vs Error

```python
# By default, a failing test ERRORS the dbt run
# You can make tests WARN instead (pipeline continues but you're notified)

severity_yaml = """
models:
  - name: stg_orders
    columns:
      - name: amount
        tests:
          - not_null:
              severity: warn    # Missing amounts: warn, don't fail the run
          - positive_values:
              severity: error   # Negative amounts: hard error!

      - name: region
        tests:
          - accepted_values:
              values: ['NORTH','SOUTH','EAST','WEST']
              severity: warn    # Unknown regions: just warn — might be new regions
"""
print(severity_yaml)
```

---

## 💻 Example 5: dbt Documentation

```python
# Add descriptions to models and columns in schema.yml

doc_yaml = """
version: 2

models:
  - name: fct_sales
    description: >
      Daily sales fact table. Contains one row per completed order,
      enriched with customer and product dimension attributes.
      Updated daily via the morning ETL run.
    meta:
      owner: "Data Engineering Team"
      sla: "Before 8am UTC"

    columns:
      - name: order_id
        description: "Unique identifier for each order. Natural key from ERP system."
      - name: customer_segment
        description: |
          Customer tier based on annual spend:
          - PLATINUM: > $50,000/year
          - GOLD:     $10,000-$50,000/year
          - SILVER:   < $10,000/year
"""
print(doc_yaml)

# Generate and serve docs:
print("\nTerminal commands:")
print("dbt docs generate   → builds the docs site")
print("dbt docs serve      → opens browser at http://localhost:8080")
print("\nFeatures:")
print("- Interactive DAG of all model dependencies")
print("- Column-level descriptions and types")
print("- Test results per model")
print("- Source freshness status")
```

---

## 🏭 Summary

| Test Type           | File Location         | Use For                               |
| ------------------- | --------------------- | ------------------------------------- |
| Schema tests        | `schema.yml`          | Standard checks: not_null, unique, FK |
| `accepted_values`   | `schema.yml`          | Validate allowed values               |
| `relationships`     | `schema.yml`          | Referential integrity between models  |
| Custom generic test | `tests/generic/*.sql` | Reusable business rule tests          |
| Singular test       | `tests/*.sql`         | One-off complex pipeline checks       |
| `severity: warn`    | `schema.yml`          | Non-critical — warn but don't fail    |

---

## ⚠️ Common Mistakes

```python
mistakes = [
    "No unique test on PKs → duplicate rows silently corrupt aggregations",
    "No not_null on FK columns → LEFT JOINs silently add nulls to facts",
    "Only running tests in prod → test in dev first!",
    "No descriptions → undocumented models = tribal knowledge",
]
for m in mistakes:
    print(f"❌ {m}")
```
