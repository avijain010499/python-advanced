# 🔗 Joins in PySpark (ETL Context)

---

## 🤔 What Are Joins?

Joins combine rows from two DataFrames based on a matching condition — the same as SQL JOINs. In ETL, you constantly join fact tables with dimension tables, or enrich source records with lookup data.

> ⚠️ **Joins are the most expensive operation in Spark.** They require data from different partitions/machines to be shuffled together. Understanding _how_ to join efficiently is critical for production ETL.

---

## 🧱 Join Types

```
INNER  - Keep rows that match in BOTH tables
LEFT   - Keep ALL left rows + matches (null for non-matches from right)
RIGHT  - Keep ALL right rows + matches
OUTER  - Keep ALL rows from BOTH (null where no match)
LEFT_SEMI  - Keep left rows that HAVE a match (no right columns returned)
LEFT_ANTI  - Keep left rows that DO NOT have a match (find orphans!)
CROSS  - Every left row × every right row (cartesian product — DANGEROUS!)
```

---

## 💻 Example 1: Basic Joins

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

orders = spark.createDataFrame([
    ("ORD-001", 101, 1500.0),
    ("ORD-002", 102,  200.0),
    ("ORD-003", 101,  800.0),
    ("ORD-004", 999,  300.0),   # customer 999 doesn't exist!
], ["order_id", "customer_id", "amount"])

customers = spark.createDataFrame([
    (101, "Alice", "NORTH"),
    (102, "Bob",   "SOUTH"),
    (103, "Charlie","EAST"),   # customer 103 has no orders
], ["customer_id", "name", "region"])

# INNER — only matched rows (ORD-004 and Charlie dropped)
inner = orders.join(customers, on="customer_id", how="inner")
inner.show()

# LEFT — all orders, null for unmatched customer (ORD-004 kept, region=null)
left = orders.join(customers, on="customer_id", how="left")
left.show()

# LEFT_ANTI — find orphaned orders (no matching customer)
orphans = orders.join(customers, on="customer_id", how="left_anti")
orphans.show()  # Shows ORD-004 — useful for data quality checks!

# LEFT_SEMI — orders that DO have a customer (like INNER but no right cols)
has_customer = orders.join(customers, on="customer_id", how="left_semi")
has_customer.show()
```

---

## 💻 Example 2: Join on Different Column Names

```python
# When column names differ between left and right tables
# Use col("df.colname") to be explicit and avoid ambiguity

orders2 = spark.createDataFrame([
    ("ORD-001", 101), ("ORD-002", 102)
], ["order_id", "cust_id"])

customers2 = spark.createDataFrame([
    (101, "Alice"), (102, "Bob")
], ["customer_id", "name"])

result = orders2.join(
    customers2,
    orders2["cust_id"] == customers2["customer_id"],   # Explicit condition
    how="left"
).drop("customer_id")   # Drop the duplicate key from right side

result.show()
```

---

## 💻 Example 3: Multi-Key Join (Composite Key)

```python
# Join on multiple columns simultaneously
sales = spark.createDataFrame([
    ("Widget","NORTH",1500.0),
    ("Gadget","SOUTH", 800.0),
], ["product","region","revenue"])

targets = spark.createDataFrame([
    ("Widget","NORTH",2000.0),
    ("Gadget","SOUTH",1000.0),
], ["product","region","target"])

# Both 'product' AND 'region' must match
result = sales.join(targets, on=["product","region"], how="left")
result.withColumn("pct", (col("revenue")/col("target")*100).cast("int")).show()
```

---

## 💻 Example 4: Broadcast Join — Small Table Optimization ⭐

When one table is small (fits in memory), **broadcast it** to avoid an expensive shuffle:

```python
from pyspark.sql.functions import broadcast

# dim_region is small (100 rows) — broadcast to every executor
dim_region = spark.createDataFrame([
    ("NORTH","North America","USD"),
    ("SOUTH","South America","BRL"),
], ["region","region_name","currency"])

# ✅ Broadcast hint: sends dim_region to all executors — no shuffle needed!
enriched = fact_sales.join(
    broadcast(dim_region),   # This prevent the expensive shuffle
    on="region",
    how="left"
)
enriched.show()

# Rule of thumb: broadcast tables < 10MB (or as configured in:
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m"))
```

---

## 💻 Example 5: Handling Column Name Conflicts After Join

```python
# Both tables might have columns with the same name (e.g., both have "status")
orders = spark.createDataFrame([("ORD-1", 101, "PENDING")], ["order_id","cust_id","status"])
customers = spark.createDataFrame([(101,"Alice","ACTIVE")],  ["cust_id","name","status"])

# After join, BOTH 'status' columns exist — ambiguous!
joined = orders.join(customers, on="cust_id", how="left")
# joined.select("status")  ← AnalysisException: Ambiguous reference!

# ✅ Fix 1: Rename before join
orders_r = orders.withColumnRenamed("status","order_status")
customers_r = customers.withColumnRenamed("status","customer_status")
fixed = orders_r.join(customers_r, on="cust_id", how="left")

# ✅ Fix 2: Explicit selection after join
joined2 = orders.alias("o").join(customers.alias("c"), on="cust_id", how="left")
fixed2 = joined2.select(
    col("o.order_id"), col("o.status").alias("order_status"),
    col("c.name"),     col("c.status").alias("customer_status")
)
fixed2.show()
```

---

## 💻 Example 6: Full ETL Star Schema Join

```python
from pyspark.sql.functions import broadcast

# Load all dimension tables (typically small → broadcast)
dim_customer = spark.read.parquet("dim/customer/")
dim_product  = spark.read.parquet("dim/product/")
dim_region   = spark.read.parquet("dim/region/")

# Load fact table (large)
fact_orders  = spark.read.parquet("fact/orders/")

# Enrich fact with all dimensions using broadcast joins
fact_enriched = (
    fact_orders
    .join(broadcast(dim_customer), on="customer_id", how="left")
    .join(broadcast(dim_product),  on="product_id",  how="left")
    .join(broadcast(dim_region),   on="region_code",  how="left")
)

# Data quality: check for missing dimension lookups
missing = fact_enriched.filter(col("customer_name").isNull()).count()
print(f"⚠️  Orders with missing customer dimension: {missing:,}")

fact_enriched.write.mode("overwrite").parquet("output/fact_orders_enriched/")
```

---

## 🏭 ETL Use Cases Summary

| Join Type     | ETL Use Case                                 |
| ------------- | -------------------------------------------- |
| `inner`       | Strict match — only valid linked records     |
| `left`        | Enrich facts with dimensions, keep all facts |
| `left_anti`   | Find orphaned records (data quality)         |
| `left_semi`   | Filter facts by existence in dimension       |
| `broadcast()` | Dimension table << Fact table                |

---

## ⚠️ Common Mistakes

```python
# ❌ Joining large tables without broadcast → expensive shuffle
fact.join(dim, on="key")   # Both sides shuffled!

# ✅ Broadcast the small dimension
fact.join(broadcast(dim), on="key")

# ❌ Forgetting ambiguous columns after joining tables with same col names
joined.select("status")   # AnalysisException: Ambiguous reference!

# ✅ Use aliases to disambiguate
joined = orders.alias("o").join(customers.alias("c"), ...)
joined.select("o.status", "c.status")
```
