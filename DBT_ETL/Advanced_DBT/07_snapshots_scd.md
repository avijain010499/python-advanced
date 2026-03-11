# 📸 Snapshots — SCD Type 2 History in dbt

---

## 🤔 What Are Snapshots?

In data warehousing, **slowly changing dimensions (SCD)** track how dimension values change over time. For example:

- A customer moves from region NORTH to SOUTH
- A product changes its category

**SCD Type 2** keeps a full history row for each change with `valid_from` / `valid_to` dates.

**dbt Snapshots** automate this — you write a simple `SELECT`, dbt handles the history tracking.

---

## 💻 Example 1: Basic Snapshot

```python
# File: snapshots/customer_snapshot.sql
customer_snapshot = """
{% snapshot customer_snapshot %}

{{
  config(
    target_schema  = 'snapshots',
    unique_key     = 'customer_id',
    strategy       = 'timestamp',
    updated_at     = 'updated_at',    -- Column that changes when row is updated
  )
}}

SELECT
    customer_id,
    customer_name,
    email,
    region,
    segment,
    updated_at

FROM {{ source('raw', 'customers') }}

{% endsnapshot %}
"""
print(customer_snapshot)

print("Run: dbt snapshot")
print("""
What dbt creates (customer_snapshot table):
customer_id | customer_name | region | segment | dbt_valid_from      | dbt_valid_to        | dbt_scd_id
101         | Alice         | NORTH  | Gold    | 2024-01-01 00:00:00 | 2024-07-15 00:00:00 | abc123...
101         | Alice         | SOUTH  | Plat    | 2024-07-15 00:00:00 | NULL                | def456...
                                                                        ↑ NULL = current record!
""")
```

---

## 💻 Example 2: Check Strategy (No `updated_at` Column)

```python
# If your source has no updated_at, use 'check' strategy
# dbt compares specific columns and detects changes

check_strategy = """
{% snapshot product_snapshot %}

{{
  config(
    target_schema = 'snapshots',
    unique_key    = 'product_id',
    strategy      = 'check',
    check_cols    = ['category', 'price', 'status'],  -- Watch these for changes
    -- check_cols = 'all'  ← check ALL columns
  )
}}

SELECT
    product_id,
    product_name,
    category,
    price,
    status

FROM {{ source('raw', 'products') }}

{% endsnapshot %}
"""
print(check_strategy)
```

---

## 💻 Example 3: Using Snapshots in Models — Point-in-Time Joins

```python
# After running dbt snapshot, use the snapshot in a model
# to join facts with the dimension value that was CURRENT at transaction time

point_in_time_sql = """
-- Get the customer's segment AT THE TIME of the order (not current segment)
SELECT
    o.order_id,
    o.order_date,
    o.amount,
    c.region,
    c.segment     -- The segment the customer had WHEN the order was placed

FROM {{ ref('fct_orders') }} AS o

LEFT JOIN {{ ref('customer_snapshot') }} AS c
    ON  o.customer_id  = c.customer_id
    AND o.order_date  >= c.dbt_valid_from
    AND (o.order_date < c.dbt_valid_to OR c.dbt_valid_to IS NULL)
    --  ↑ This is the SCD Type 2 join pattern!
    --  Gets the customer version that was active on the order_date
"""
print(point_in_time_sql)
```

---

## 💻 Example 4: Snapshot Metadata Columns

```python
snapshot_columns = {
    "dbt_scd_id":     "Unique hash ID for each snapshot row",
    "dbt_updated_at": "When dbt last updated this row",
    "dbt_valid_from": "When this version of the record became active",
    "dbt_valid_to":   "When this version expired (NULL = currently active)",
}
print("Columns dbt adds automatically to snapshot tables:")
for col, desc in snapshot_columns.items():
    print(f"  {col:<20} → {desc}")

print("""
Query only CURRENT records:
  SELECT * FROM {{ ref('customer_snapshot') }}
  WHERE dbt_valid_to IS NULL    -- Only current versions

Query ALL history:
  SELECT * FROM {{ ref('customer_snapshot') }}
  ORDER BY customer_id, dbt_valid_from
""")
```

---

## 💻 Example 5: Snapshot Schedule in Production

```python
snapshot_pipeline = """
Production snapshot schedule pattern:
1. dbt source freshness          -- Verify source data arrived
2. dbt snapshot                 -- Detect and record any changes
3. dbt run --select +fct_sales  -- Rebuild fact using updated snapshots
4. dbt test                     -- Validate data quality

Run via Prefect/Airflow/dbt Cloud schedule daily before 8am.
"""
print(snapshot_pipeline)

# Common pitfall: running snapshot AFTER models that depend on it
print("""
❌ Wrong order:
  1. dbt run    (builds models with stale snapshot data!)
  2. dbt snapshot

✅ Correct order:
  1. dbt snapshot   (detect changes FIRST)
  2. dbt run        (build models with fresh snapshot data)
""")
```

---

## ⚠️ Common Mistakes

```python
mistakes = [
    ("Dropping and recreating snapshot table", "You lose ALL history! Never recreate snapshots"),
    ("Running models before snapshot",         "Models use stale dimension data"),
    ("Using check strategy on wide tables",    "Checks ALL rows every run = slow; prefer timestamp strategy"),
    ("Not querying dbt_valid_to IS NULL",      "Accidentally joins ALL history = row explosion in fact"),
]
for mistake, fix in mistakes:
    print(f"❌ {mistake}")
    print(f"✅ Fix: {fix}\n")
```
