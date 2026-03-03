# 📂 Partitioning & Bucketing in PySpark (ETL Context)

---

## 🤔 What Is Partitioning?

When Spark reads or writes data, it divides it into **partitions** — chunks that can be processed in parallel across executors. How you partition your data dramatically affects performance.

There are two types to understand:

1. **Spark partitions** — how data is divided in memory during processing
2. **File-system partitioning** — how files are organized on disk (Hive-style)

> 💡 **The sweet spot**: Aim for partition sizes of **128MB–256MB** on disk and **tasks that run in 1–5 minutes**. Too many small partitions = overhead. Too few large = wasted parallelism.

---

## 🧱 Key Concepts

| Concept                        | What It Means                                             |
| ------------------------------ | --------------------------------------------------------- |
| `repartition(n)`               | Shuffle data into exactly n partitions (full shuffle)     |
| `coalesce(n)`                  | Reduce partitions WITHOUT a full shuffle (more efficient) |
| `partitionBy("col")`           | On write: organize output files into subdirectories       |
| `spark.sql.shuffle.partitions` | Number of partitions after a shuffle (default: 200)       |
| Data skew                      | One partition is much larger than others → bottleneck     |

---

## 💻 Example 1: Repartition vs Coalesce

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

df = spark.range(1000000)  # DataFrame of 1M rows

print(f"Default partitions: {df.rdd.getNumPartitions()}")  # e.g., 4

# repartition(n): full shuffle → increases OR decreases partition count
# Use when: you need to INCREASE partitions, or evenly redistribute skewed data
df_repart = df.repartition(10)
print(f"After repartition(10): {df_repart.rdd.getNumPartitions()}")  # 10

# repartition by column: all rows with same value go to the same partition
# Useful before a join or groupBy on that column
df_by_region = df_orders.repartition(col("region"))

# coalesce(n): reduces partitions WITHOUT a full shuffle (faster!)
# Use when: you need to REDUCE partitions (e.g., before writing a small output)
df_small = df_repart.coalesce(2)
print(f"After coalesce(2): {df_small.rdd.getNumPartitions()}")  # 2

# Rule of thumb:
# Need MORE partitions → repartition()
# Need FEWER partitions → coalesce()
```

---

## 💻 Example 2: File System partitionBy on Write

```python
from pyspark.sql.functions import year, month

df = spark.read.parquet("raw/sales/")

# Add year and month columns (needed for partitionBy)
df = df \
    .withColumn("year",  year(col("order_date"))) \
    .withColumn("month", month(col("order_date")))

# Write partitioned by year and month
# Creates: output/sales/year=2024/month=1/part-*.parquet
#                                  year=2024/month=2/part-*.parquet
#                                  ...
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("output/sales_partitioned/")

# WHY PARTITION ON DISK?
# When you query by date later, Spark only reads the relevant partitions:
df_jan = spark.read.parquet("output/sales_partitioned/").filter(
    (col("year") == 2024) & (col("month") == 1)
)
# ↑ Spark skips all other month directories — "partition pruning"!
```

---

## 💻 Example 3: Set Shuffle Partitions for Performance

```python
# Default: 200 shuffle partitions — too many for small data!
# 200 partitions on 100MB of data → 0.5MB per partition → excessive overhead

# For local development (small data):
spark.conf.set("spark.sql.shuffle.partitions", "4")

# For production (large data — typically 2-4x number of total executor cores):
# 50 executors × 4 cores each = 200 cores → 400-800 shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Or enable Adaptive Query Execution (Spark 3+) — automatically adjusts!
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# AQE will reduce 200 shuffle partitions down to what's actually needed

result = df.groupBy("region").sum("revenue")   # AQE optimizes this shuffle
result.show()
```

---

## 💻 Example 4: Detect and Fix Data Skew

```python
# Skew: one partition has way more data than others → slow "straggler" task
from pyspark.sql.functions import col, count

# Check partition size distribution (useful for debugging)
df.withColumn("partition_id", spark_partition_id()) \
  .groupBy("partition_id") \
  .count() \
  .orderBy(col("count").desc()) \
  .show()

# Skew fix for joins: add random salt to the skewed key
# If region "NORTH" has 90% of data, split it into NORTH_0 through NORTH_9
import pyspark.sql.functions as F

SALT_FACTOR = 10

# Salt the large table
df_large = df_orders.withColumn(
    "salted_key",
    F.concat(col("region"), F.lit("_"), (F.rand() * SALT_FACTOR).cast("int").cast("string"))
)

# Explode the small table (replicate each row SALT_FACTOR times)
df_small = df_regions.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(SALT_FACTOR)])))
df_small = df_small.withColumn("salted_key", F.concat(col("region"), F.lit("_"), col("salt").cast("string")))

# Join on the salted key
result = df_large.join(broadcast(df_small), on="salted_key", how="left").drop("salted_key","salt")
```

---

## 💻 Example 5: ETL — Optimal Write Pattern

```python
from pyspark.sql.functions import col

# Before writing a large result to Parquet:
# 1. Remove shuffle-heavy ops already done (avoid re-shuffle)
# 2. Repartition to target ~128-256MB per output file
# 3. Partition on disk by date for efficient downstream reads

target_partitions = 20  # Tune based on output size / 128MB

(df_transformed
 .repartition(target_partitions)          # In-memory repartition
 .write
 .mode("overwrite")
 .partitionBy("year", "month")            # On-disk partition structure
 .option("compression", "snappy")
 .parquet("output/sales_final/")
)
```

---

## 🏭 ETL Use Cases Summary

| Technique                     | When to Use                                               |
| ----------------------------- | --------------------------------------------------------- |
| `repartition(n)`              | Increase partitions or evenly redistribute skewed data    |
| `coalesce(n)`                 | Reduce partitions before writing to avoid many tiny files |
| `partitionBy("col")` on write | Partition data lake by date/region for pruning            |
| AQE enabled                   | Let Spark automatically optimize shuffle partitions       |
| Salting                       | Fix data skew in joins with hot keys                      |

---

## ⚠️ Common Mistakes

```python
# ❌ Default 200 shuffle partitions for small data = 200 empty tasks!
result = df.groupBy("region").sum("revenue")   # Creates 200 tiny partitions!

# ✅ Set appropriately
spark.conf.set("spark.sql.shuffle.partitions", "4")  # Or enable AQE

# ❌ Using repartition() to reduce (causes full shuffle — wasteful!)
df.repartition(1)   # Full shuffle just to get 1 partition!

# ✅ Use coalesce() to reduce
df.coalesce(1)      # No shuffle — just merge existing partitions

# ❌ Too many tiny output files (small file problem)
df.repartition(500).write.parquet("output/")  # 500 tiny files!
# ✅ Target 128-256MB per file
df.coalesce(10).write.parquet("output/")
```
