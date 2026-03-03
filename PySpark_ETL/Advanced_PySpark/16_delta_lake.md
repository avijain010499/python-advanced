# 🏔️ Delta Lake in PySpark (ETL Context)

---

## 🤔 What Is Delta Lake?

**Delta Lake** is an open-source storage layer that adds **ACID transactions**, **versioning**, and **upserts** to Parquet files on data lakes (S3, ADLS, GCS).

Without Delta Lake, data lakes have painful problems:

- No atomic writes → partial failures leave corrupt data
- No upserts → have to reload entire tables to fix one record
- No history → can't audit or roll back changes

> 💡 **Think of Delta Lake as giving your data lake the reliability of a database while keeping the scale and cost of cloud storage.** It's the standard format in Databricks and increasingly in AWS/Azure pipelines.

---

## 🧱 Setup

```bash
pip install delta-spark
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaLakeETL") \
    .master("local[*]") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

---

## 💻 Example 1: Write and Read Delta Tables

```python
from pyspark.sql.functions import col

df = spark.createDataFrame([
    ("ORD-001", 101, 1500.0, "COMPLETE"),
    ("ORD-002", 102,  200.0, "PENDING"),
    ("ORD-003", 103,  800.0, "COMPLETE"),
], ["order_id","customer_id","amount","status"])

# Write as Delta (instead of Parquet)
df.write.format("delta").mode("overwrite").save("/tmp/delta/sales/")

# Read back
df_delta = spark.read.format("delta").load("/tmp/delta/sales/")
df_delta.show()

# Or as a managed table
df.write.format("delta").saveAsTable("sales_delta")
spark.sql("SELECT * FROM sales_delta").show()
```

---

## 💻 Example 2: MERGE (Upsert) — The Most Important Delta Feature

```python
from delta.tables import DeltaTable

# Load target table
delta_table = DeltaTable.forPath(spark, "/tmp/delta/sales/")

# New/updated records from source
updates = spark.createDataFrame([
    ("ORD-001", 101, 1750.0, "COMPLETE"),  # Updated amount
    ("ORD-004", 104, 500.0,  "PENDING"),   # New record
], ["order_id","customer_id","amount","status"])

# MERGE: update existing records, insert new ones
delta_table.alias("target").merge(
    updates.alias("source"),
    condition="target.order_id = source.order_id"
).whenMatchedUpdateAll(                # If match → update all columns
).whenNotMatchedInsertAll(             # If no match → insert new row
).execute()

delta_table.toDF().show()
# ORD-001 now has amount=1750.0, ORD-004 is new
```

---

## 💻 Example 3: Time Travel — Audit and Rollback

```python
# Delta keeps a transaction log — you can query any past version!

# Read the original version (before any updates)
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta/sales/")
df_v0.show()

# Read the state at a specific timestamp
df_ts = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-15 10:00:00") \
    .load("/tmp/delta/sales/")

# View full version history
delta_table = DeltaTable.forPath(spark, "/tmp/delta/sales/")
delta_table.history().select("version","timestamp","operation").show()
# +-------+--------------------+---------+
# |version|timestamp           |operation|
# +-------+--------------------+---------+
# |1      |2024-01-15 10:05:00|MERGE    |
# |0      |2024-01-15 10:00:00|WRITE    |
# +-------+--------------------+---------+

# Rollback to a previous version
spark.sql("RESTORE TABLE sales_delta TO VERSION AS OF 0")
```

---

## 💻 Example 4: Schema Enforcement and Evolution

```python
# Delta enforces schema by default — prevents accidental bad writes
bad_df = spark.createDataFrame([("ORD-005", "not-a-number")], ["order_id","amount"])

try:
    bad_df.write.format("delta").mode("append").save("/tmp/delta/sales/")
except Exception as e:
    print(f"Blocked: {e}")  # Schema mismatch! amount must be DoubleType

# Schema evolution: add new columns with mergeSchema option
new_df = spark.createDataFrame([("ORD-006",105,900.0,"COMPLETE","promo_code_A")],
    ["order_id","customer_id","amount","status","promo_code"])

new_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema","true") \   # Allow adding the new "promo_code" column
    .save("/tmp/delta/sales/")
# Existing rows will have promo_code = null
```

---

## 💻 Example 5: Optimize and Vacuum

```python
# OPTIMIZE: compact small files into larger ones (better read performance)
spark.sql("OPTIMIZE delta.`/tmp/delta/sales/`")

# Z-ORDER: order data within files by a column for faster range queries
spark.sql("OPTIMIZE delta.`/tmp/delta/sales/` ZORDER BY (order_id)")

# VACUUM: clean up old file versions (default retention: 7 days)
# WARNING: After vacuum you CANNOT time-travel to those old versions!
spark.sql("VACUUM delta.`/tmp/delta/sales/` RETAIN 168 HOURS")  # 7 days
```

---

## 🏭 ETL Use Cases Summary

| Delta Feature      | ETL Use Case                                   |
| ------------------ | ---------------------------------------------- |
| ACID writes        | Reliable writes — no partial failures          |
| MERGE (upsert)     | Incremental ETL — update existing + insert new |
| Time travel        | Data audits, regulatory compliance, rollback   |
| Schema enforcement | Catch source schema drift immediately          |
| OPTIMIZE + ZORDER  | Query performance on large analytical tables   |

---

## ⚠️ Common Mistakes

```python
# ❌ No RETAIN in VACUUM — uses 7-day default but you might lose history you needed
delta_table.vacuum()   # BE CAREFUL — irreversible!

# ✅ Always be explicit
delta_table.vacuum(retentionHours=168)  # Keep 7 days

# ❌ MERGE without a unique key condition → duplicates!
delta_table.merge(updates, "1=1")   # Matches ALL rows to ALL updates!

# ✅ Match on your natural/surrogate key
delta_table.merge(updates, "target.order_id = source.order_id")
```
