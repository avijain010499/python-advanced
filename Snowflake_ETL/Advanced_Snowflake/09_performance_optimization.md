# ⚡ Performance Optimization in Snowflake

---

## 🤔 Why Optimize?

Every Snowflake query consumes **credits** (compute cost). Optimizing means:

- Less data scanned → faster queries and lower cost
- Right warehouse size → no over-spending
- Good table design → the optimizer can prune data

---

## 💻 Example 1: Clustering Keys — Prune Large Tables

```python
clustering_sql = """
-- Problem: a 500GB fact table with queries always filtering on ORDER_DATE and REGION
-- Without clustering: Snowflake scans ALL micro-partitions (expensive!)
-- With clustering: Snowflake scans only relevant micro-partitions

-- Add a clustering key
ALTER TABLE ETL_DB.ANALYTICS.FCT_ORDERS
    CLUSTER BY (ORDER_DATE, REGION);
-- Snowflake automatically maintains the clustering over time

-- Check clustering effectiveness
SELECT SYSTEM$CLUSTERING_INFORMATION('ETL_DB.ANALYTICS.FCT_ORDERS', '(ORDER_DATE, REGION)');
-- "average_depth" should be close to 1.0 (well-clustered)

-- Manual re-clustering (usually not needed — Snowflake auto-clusters):
ALTER TABLE ETL_DB.ANALYTICS.FCT_ORDERS RECLUSTER;
"""
print(clustering_sql)

when_to_cluster = {
    "Cluster when": [
        "Table > 1TB and queries always filter on same 1-2 columns",
        "Partition pruning isn't happening (many micro-partitions scanned)",
    ],
    "Don't cluster when": [
        "Table < 500GB (auto micro-partitioning is sufficient)",
        "Queries filter on many different columns (no single clustering benefit)",
        "Table is frequently updated (re-clustering overhead)",
    ],
}
for scenario, points in when_to_cluster.items():
    print(f"\n  {scenario}:")
    for p in points:
        print(f"    • {p}")
```

---

## 💻 Example 2: Query Profile — Find the Bottleneck

```python
explain_sql = """
-- Step 1: Run EXPLAIN to see the query plan BEFORE running
EXPLAIN
SELECT REGION, SUM(AMOUNT) AS TOTAL
FROM ETL_DB.ANALYTICS.FCT_ORDERS
WHERE ORDER_DATE >= '2024-01-01'
GROUP BY REGION;

-- Step 2: After running, check in Snowsight:
--   Activity → Query History → Click query → Query Profile
-- Look for:
--   - "Bytes scanned" — high value = full table scan, consider clustering
--   - "Partitions pruned" vs "Partitions total" → 90% pruned = good!
--   - "Spillage to disk" → not enough memory → use bigger warehouse

-- Step 3: Use QUERY_HISTORY view for programmatic analysis
SELECT
    QUERY_TEXT,
    TOTAL_ELAPSED_TIME / 1000                AS DURATION_SECONDS,
    BYTES_SCANNED / 1024 / 1024 / 1024      AS GB_SCANNED,
    PARTITIONS_TOTAL,
    PARTITIONS_SCANNED,
    1 - (PARTITIONS_SCANNED / PARTITIONS_TOTAL) AS PRUNE_RATE,
    CREDITS_USED_CLOUD_SERVICES
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
  AND TOTAL_ELAPSED_TIME > 60000    -- Queries that took > 60s
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 20;
"""
print(explain_sql)
```

---

## 💻 Example 3: Virtual Warehouse Sizing & Auto-Suspend

```python
warehouse_sql = """
-- Create dedicated warehouses per workload (different schedules, different sizes)
CREATE WAREHOUSE IF NOT EXISTS ETL_WH
    WAREHOUSE_SIZE = 'MEDIUM'          -- 4 nodes (XS=1, S=2, M=4, L=8, XL=16)
    AUTO_SUSPEND   = 60                -- Suspend after 60 seconds of idle
    AUTO_RESUME    = TRUE              -- Resume automatically on query arrival
    MAX_CLUSTER_COUNT = 3             -- Multi-cluster: auto-scale up to 3 warehouses
    MIN_CLUSTER_COUNT = 1             -- Scale back to 1 when load drops
    SCALING_POLICY  = 'ECONOMY'       -- Prefer cost over speed
    COMMENT = 'ETL transforms — runs 5am-7am UTC daily';

-- BI/analytics read queries (concurrent users → multi-cluster)
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND   = 120
    MAX_CLUSTER_COUNT = 5             -- Scale to 5 for concurrent BI users
    SCALING_POLICY  = 'STANDARD';

-- Suspend/resume manually
ALTER WAREHOUSE ETL_WH SUSPEND;
ALTER WAREHOUSE ETL_WH RESUME;

-- Resize dynamically during heavy loads
ALTER WAREHOUSE ETL_WH SET WAREHOUSE_SIZE = 'LARGE';    -- Scale up for big job
-- ... run big job ...
ALTER WAREHOUSE ETL_WH SET WAREHOUSE_SIZE = 'MEDIUM';   -- Scale back down
"""
print(warehouse_sql)
```

---

## 💻 Example 4: Result Cache & Query Best Practices

```python
optimizations = {
    "Result cache": """
-- Snowflake caches query results for 24h
-- Exact same query → returns instantly, no compute cost!
SELECT SUM(AMOUNT) FROM FCT_ORDERS WHERE ORDER_DATE = '2024-01-15';
-- Run again within 24h → no credits used (result reused)

-- Disable result cache for benchmarking:
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
    """,

    "Column pruning — only SELECT what you need": """
-- ❌ BAD: SELECT * scans all columns (Snowflake is columnar!)
SELECT * FROM FCT_ORDERS;

-- ✅ GOOD: Only select needed columns
SELECT ORDER_ID, AMOUNT, REGION FROM FCT_ORDERS;
    """,

    "Filter early with CTEs": """
-- ❌ BAD: Join full tables then filter
SELECT * FROM FCT_ORDERS o JOIN DIM_CUSTOMER c ON o.CUSTOMER_ID = c.CUSTOMER_ID
WHERE o.ORDER_DATE >= '2024-01-01';

-- ✅ GOOD: Filter BEFORE join
WITH RECENT_ORDERS AS (
    SELECT * FROM FCT_ORDERS WHERE ORDER_DATE >= '2024-01-01'
)
SELECT r.*, c.CUSTOMER_NAME
FROM RECENT_ORDERS r JOIN DIM_CUSTOMER c ON r.CUSTOMER_ID = c.CUSTOMER_ID;
    """,
}
for tip, example in optimizations.items():
    print(f"✅ {tip}")
    print(example)
```

---

## 🏭 Performance Checklist

| Optimization                           | Impact                                     |
| -------------------------------------- | ------------------------------------------ |
| Cluster large tables on filter columns | ⭐⭐⭐⭐⭐ Fewer partitions scanned        |
| `AUTO_SUSPEND` on all warehouses       | ⭐⭐⭐⭐⭐ Eliminate idle credit waste     |
| Right-size warehouse for workload      | ⭐⭐⭐⭐ Don't pay for XL when S is enough |
| Filter before joining (CTE pattern)    | ⭐⭐⭐ Reduce rows earlier                 |
| SELECT only needed columns             | ⭐⭐⭐ Columnar storage — column pruning   |
| Use result cache for repeated reads    | ⭐⭐ Free for identical queries            |
| Multi-cluster for concurrent users     | ⭐⭐ Prevents BI query queuing             |
