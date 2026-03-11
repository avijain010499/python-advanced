# 🏆 dbt Best Practices for ETL

---

## 🤔 Why Best Practices Matter?

A dbt project that follows conventions is easy to onboard, debug, and scale. A project without conventions becomes a labyrinth of mystery SQL.

---

## 💻 Example 1: Project Structure Convention

```python
project_structure = """
my_etl_project/
├── models/
│   ├── staging/                    # One model per source table — prefix: stg_
│   │   ├── _sources.yml            # Source definitions
│   │   ├── stg_orders.sql
│   │   └── stg_customers.sql
│   │
│   ├── intermediate/               # Business logic — prefix: int_
│   │   └── int_orders_enriched.sql
│   │
│   └── marts/
│       ├── core/                   # Cross-domain facts/dims
│       │   ├── fct_orders.sql      # Facts: fct_ prefix
│       │   └── dim_customer.sql    # Dimensions: dim_ prefix
│       └── finance/                # Domain-specific marts
│           └── fct_revenue.sql
│
├── snapshots/                      # SCD history snapshots
├── seeds/                          # Reference/mapping CSVs
├── macros/                         # Shared Jinja functions
├── analyses/                       # Ad-hoc SQL (not materialized)
├── tests/                          # Custom singular tests
├── dbt_project.yml
└── packages.yml
"""
print(project_structure)
```

---

## 💻 Example 2: Naming Conventions

```python
naming_conventions = {
    "stg_*":   "Staging model — clean one source table",
    "int_*":   "Intermediate model — combine/transform staging models",
    "fct_*":   "Fact table — events/transactions (grain = one event)",
    "dim_*":   "Dimension table — who/what/where attributes",
    "rpt_*":   "Report model — for a specific BI dashboard",
    "base_*":  "Base model — minimal raw layer (if using base layer)",
}
print("Model naming conventions:")
for prefix, desc in naming_conventions.items():
    print(f"  {prefix:<12} → {desc}")

column_conventions = {
    "Primary keys":     "<model_name>_id  (e.g., order_id, not just 'id')",
    "Boolean cols":     "is_ prefix: is_active, is_complete",
    "Dates":            "event_date (DATE), created_at (TIMESTAMP)",
    "Amounts":          "amount_usd (always include currency unit)",
    "Foreign keys":     "Same name as in referenced dimension (customer_id)",
}
print("\nColumn naming conventions:")
for pattern, example in column_conventions.items():
    print(f"  {pattern:<18} → {example}")
```

---

## 💻 Example 3: The Staging Layer Rules

```python
staging_rules = [
    "Only SELECT from sources (never ref() to another model in staging)",
    "One staging model per source table — keep 1:1 mapping",
    "No business logic — only: rename, retype, trim, nullify",
    "Always use source() function — never hardcode schema.table",
    "Materialized as 'view' — no storage cost, always fresh",
]
print("Staging model golden rules:")
for rule in staging_rules:
    print(f"  ✅ {rule}")

good_staging = """
-- ✅ GOOD staging model
SELECT
    order_id,
    customer_id,
    UPPER(TRIM(status))           AS status,    -- Clean
    CAST(order_date AS DATE)      AS order_date, -- Cast type
    NULLIF(region, '')             AS region,    -- Normalize empty → null
    amount * 1.0                  AS amount      -- Ensure numeric
FROM {{ source('raw', 'orders') }}
WHERE order_id IS NOT NULL
"""

bad_staging = """
-- ❌ BAD staging model
SELECT
    o.order_id,
    c.customer_name,         -- WRONG: joining in staging!
    SUM(o.amount) AS total   -- WRONG: aggregating in staging!
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.customer_id  -- WRONG: hardcoded schema!
"""
print(good_staging)
print(bad_staging)
```

---

## 💻 Example 4: Performance Best Practices

```python
perf_tips = {
    "Use incremental for large facts": """
        Rebuilding 1B row fact every day = $$$
        Incremental = only process new rows → 100x cheaper
    """,
    "Cluster/partition large tables": """
        config(
          cluster_by = ['region', 'order_date']   -- Snowflake
          partition_by = {'field': 'order_date'}  -- BigQuery
        )
    """,
    "Reduce model fan-out": """
        Avoid 10 models all joining to dim_customer independently.
        Create int_orders_with_customer once, ref() from multiple marts.
    """,
    "Use ephemeral for simple CTEs": """
        Don't materialize intermediate models that are just simple SELECTs.
        config(materialized='ephemeral') → becomes an inline CTE.
    """,
}
for tip, example in perf_tips.items():
    print(f"✅ {tip}")
    print(example)
```

---

## 💻 Example 5: Complete Production Checklist

```python
checklist = {
    "Before developing": [
        "dbt debug — verify warehouse connection",
        "dbt deps  — install packages.yml dependencies",
        "dbt source freshness — confirm source data is available",
    ],
    "Development": [
        "Follow staging→intermediate→marts layer structure",
        "Use ref() and source() — never hardcode table names",
        "Add not_null and unique tests to every PK/FK column",
        "Add model description in schema.yml",
    ],
    "Before PR": [
        "dbt run --select state:modified+  — test your changes",
        "dbt test --select state:modified+ — pass all tests",
        "dbt docs generate                  — update docs",
    ],
    "Production schedule (daily)": [
        "1. dbt source freshness",
        "2. dbt snapshot",
        "3. dbt build (= seed + run + test in one command)",
        "4. Alert on failure",
    ],
}
for stage, steps in checklist.items():
    print(f"\n📌 {stage}:")
    for step in steps:
        print(f"   • {step}")
```
