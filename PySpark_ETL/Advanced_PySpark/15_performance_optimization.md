# ⚡ Performance Optimization in PySpark (ETL Context)

---

## 🤔 Why Optimize PySpark?

A slow ETL job costs money (cluster hours) and delays data freshness. The most common performance killers are:

- Excessive shuffles (data moving across the network)
- Data skew (one executor does 90% of the work)
- No caching (same computation repeated)
- Loading too much data (no pushdown)

> 💡 **Golden Rule**: The fastest Spark job is one that processes the least data, shuffles the least data, and fits the most in memory.

---

## 💻 Example 1: Predicate Pushdown — Filter at the Source

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("ETL-Optimized") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# ❌ Load everything, then filter — network and memory waste!
df_all = spark.read.parquet("data/sales/")       # Reads ALL partitions
df_filtered = df_all.filter(col("year") == 2024) # Filters after loading

# ✅ Filter pushes down to file scan — reads 2024 partition ONLY
df_good = spark.read.parquet("data/sales/year=2024/")  # Partition pruning!

# Verify with explain()
df_good.explain()   # Look for "PartitionFilters" in the plan
```

---

## 💻 Example 2: Broadcast Join — Eliminate Shuffle for Small Tables

```python
from pyspark.sql.functions import broadcast, col

# Without broadcast: BOTH tables shuffle → expensive!
result_slow = fact_orders.join(dim_region, on="region_id")

# With broadcast: dim_region sent to each executor → NO shuffle!
result_fast = fact_orders.join(broadcast(dim_region), on="region_id")

# Auto-broadcast threshold (default 10MB):
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20m")  # 20MB

# Check the plan — verify BroadcastHashJoin used:
result_fast.explain()   # Look for "BroadcastHashJoin"
```

---

## 💻 Example 3: Caching Intermediate DataFrames

```python
# Cache when a DataFrame is used in multiple downstream steps
df_clean = (
    spark.read.parquet("data/sales/")
    .filter(col("status") == "COMPLETE")
    .dropDuplicates(["order_id"])
)

# Register cache — NOT computed yet (lazy)
df_clean.cache()

# First action — computes AND stores in memory
count = df_clean.count()           # Computation happens here
print(f"Records: {count:,}")

# Subsequent actions use cached data — very fast!
summary = df_clean.groupBy("region").sum("revenue")   # Uses cache!
top10   = df_clean.orderBy(col("revenue").desc()).limit(10)   # Uses cache!

# Release memory when done
df_clean.unpersist()

# persist() gives you control over storage level:
from pyspark import StorageLevel
df_clean.persist(StorageLevel.MEMORY_AND_DISK)  # Spills to disk if RAM full
```

---

## 💻 Example 4: Adaptive Query Execution (AQE) — Spark 3+

```python
# AQE automatically optimizes queries at runtime based on actual data stats
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# AQE automatically:
# 1. Coalesces shuffle partitions (200 → actual needed count)
# 2. Handles skewed joins by splitting large partitions
# 3. Switches join strategies based on actual data sizes

# Just run your job normally — AQE works in the background
result = fact_large.join(dim_medium, on="key").groupBy("region").sum("revenue")
result.write.parquet("output/")
```

---

## 💻 Example 5: Fix Data Skew with Salting

```python
from pyspark.sql.functions import col, concat, lit, rand, explode, array

# Problem: 80% of orders have region="NORTH" → one executor does all the work
# Solution: "salt" the key to distribute the skewed data

SALT_FACTOR = 10

# Salt the large (skewed) table
df_fact_salted = df_fact \
    .withColumn("salt", (rand() * SALT_FACTOR).cast("int")) \
    .withColumn("region_salted", concat(col("region"), lit("_"), col("salt")))

# Replicate the small table for each salt value
df_dim_exploded = df_dim \
    .withColumn("salt_arr", array([lit(i) for i in range(SALT_FACTOR)])) \
    .withColumn("salt",     explode(col("salt_arr"))) \
    .withColumn("region_salted", concat(col("region"), lit("_"), col("salt"))) \
    .drop("salt_arr", "salt")

# Join on salted key
result = df_fact_salted \
    .join(df_dim_exploded, on="region_salted", how="left") \
    .drop("region_salted", "salt")

result.write.mode("overwrite").parquet("output/")
```

---

## 💻 Example 6: coalesce Before Writing — Avoid Small Files

```python
# Small files problem: 1000 files of 1KB each in a Parquet folder
# → Next read job opens 1000 file handles → slow!

# Check current partition count
print(df.rdd.getNumPartitions())   # e.g., 200

# Target: 128-256MB per output file
# If your result is 1GB → 4-8 output files is ideal
df.coalesce(8) \
  .write \
  .mode("overwrite") \
  .parquet("output/consolidated/")
```

---

## 🏭 Performance Optimization Checklist

| Optimization              | What It Does                              | Impact     |
| ------------------------- | ----------------------------------------- | ---------- |
| Partition pruning         | Read only relevant date/region partitions | ⭐⭐⭐⭐⭐ |
| Broadcast join            | Skip shuffle for small dimension tables   | ⭐⭐⭐⭐⭐ |
| AQE enabled               | Auto-optimize at run time                 | ⭐⭐⭐⭐   |
| Cache                     | Avoid recomputing used-multiple-times DFs | ⭐⭐⭐⭐   |
| Reduce shuffle partitions | Fewer partitions for small data           | ⭐⭐⭐     |
| coalesce before write     | Consolidate small output files            | ⭐⭐⭐     |
| Salting                   | Fix data skew in joins                    | ⭐⭐⭐     |

---

## ⚠️ Common Mistakes

```python
# ❌ Cache everything — wastes memory!
df1.cache(); df2.cache(); df3.cache()   # Fills cluster memory

# ✅ Cache only DataFrames used 2+ times in the pipeline

# ❌ Not unpersisting cache — memory never released!
df.cache()
df.count()
# ... hours later, cache still held in memory

# ✅ Always unpersist when finished
df.unpersist()
```
