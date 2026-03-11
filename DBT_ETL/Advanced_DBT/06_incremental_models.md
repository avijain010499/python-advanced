# 📈 Incremental Models in dbt

---

## 🤔 What Is an Incremental Model?

An **incremental model** only processes **new or changed rows** instead of rebuilding the entire table from scratch on every run.

> 💡 **When you need it**: A fact table with 1 billion rows that gets 1 million new rows per day. Rebuilding all 1 billion rows each day = wasted compute. Incrementally appending 1 million rows = fast and cheap.

---

## 💻 Example 1: Basic Incremental Model

```python
incremental_sql = """
{{
  config(
    materialized = 'incremental',
    unique_key   = 'order_id'     -- Key to MERGE on (prevents duplicates)
  )
}}

SELECT
    order_id,
    customer_id,
    amount,
    status,
    order_date,
    updated_at

FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
  -- This WHERE clause only applies on INCREMENTAL runs (not the first full run)
  WHERE updated_at > (
      SELECT COALESCE(MAX(updated_at), '1900-01-01')
      FROM {{ this }}          -- {{ this }} = the current model's table
  )
{% endif %}
"""
print(incremental_sql)

how_it_works = """
How it runs:
  Run 1 (first time):  is_incremental() = False → full SELECT, creates table
  Run 2+ (subsequent): is_incremental() = True  → filtered SELECT, MERGE/INSERT
"""
print(how_it_works)
```

---

## 💻 Example 2: Incremental Strategies

```python
strategies = """
config(
  materialized        = 'incremental',
  unique_key          = 'order_id',
  incremental_strategy = 'merge'    # Default for most warehouses
)
"""
print("Strategy options per warehouse:")
strategy_table = {
    "Snowflake":  ["merge", "delete+insert"],
    "BigQuery":   ["merge", "insert_overwrite"],
    "Redshift":   ["append", "delete+insert"],
    "Postgres":   ["append", "delete+insert"],
    "DuckDB":     ["append", "delete+insert"],
}
for wh, strats in strategy_table.items():
    print(f"  {wh:<15}: {', '.join(strats)}")

print("""
merge:          UPDATE existing rows, INSERT new rows (most common)
append:         INSERT only, no UPDATE (for immutable event tables)
delete+insert:  DELETE matching rows, re-INSERT (avoids partial updates)
insert_overwrite: Replace entire partition (BigQuery-specific)
""")
```

---

## 💻 Example 3: Partition-Based Incremental (BigQuery pattern)

```python
partition_incremental = """
{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      'field': 'order_date',
      'data_type': 'date',
      'granularity': 'day'
    }
  )
}}

SELECT
    order_id,
    customer_id,
    amount,
    order_date

FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
  WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
  -- Re-process last 3 days to catch late-arriving data
{% endif %}
"""
print("Partition-based incremental (BigQuery):")
print(partition_incremental)
```

---

## 💻 Example 4: Full Refresh Override

```python
print("When to force a full rebuild:")
full_refresh_cases = [
    "You changed the model's SQL logic significantly",
    "You need to backfill historical data",
    "Schema changed (new columns added)",
    "Data quality issue — need clean slate",
]
for case in full_refresh_cases:
    print(f"  • {case}")

print("\nCommands:")
print("  dbt run --full-refresh --select fct_orders     # Rebuild this model only")
print("  dbt run --full-refresh                         # Rebuild ALL incremental models")

full_refresh_config = """
# Prevent accidental full refresh in prod (safety net):
config(
  materialized      = 'incremental',
  unique_key        = 'order_id',
  full_refresh      = False     # Error if --full-refresh is accidentally run in prod!
)
"""
print(full_refresh_config)
```

---

## 💻 Example 5: Handling Late-Arriving Data

```python
late_data_sql = """
-- Late-arriving data: events from 3 days ago that just arrived
-- Solution: always re-process a lookback window

{{
  config(
    materialized         = 'incremental',
    unique_key           = 'event_id',
    incremental_strategy = 'merge'
  )
}}

SELECT
    event_id,
    user_id,
    event_type,
    event_ts,
    revenue

FROM {{ ref('stg_events') }}

{% if is_incremental() %}
  WHERE event_ts >= DATEADD('day', -3, CURRENT_DATE())
  -- Always re-process the last 3 days to catch late data
  -- MERGE handles updates/inserts — no duplicates!
{% endif %}
"""
print(late_data_sql)
```

---

## ⚠️ Common Mistakes

```python
mistakes = [
    ("No unique_key with append strategy", "Can create duplicate rows on retries!"),
    ("Using updated_at with no index/cluster key", "Slow subquery on huge tables"),
    ("Never running full-refresh", "Model drifts from source over time — periodically rebuild"),
    ("Not handling late-arriving data", "Events from 2 days ago may arrive today — use lookback window"),
]
for mistake, fix in mistakes:
    print(f"❌ {mistake}")
    print(f"✅ Fix: {fix}\n")
```
