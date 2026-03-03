# ⏳ Transformations vs Actions & Lazy Evaluation (ETL Context)

---

## 🤔 What Is Lazy Evaluation?

This is one of the most important concepts in PySpark. When you write:

```python
df2 = df.filter(df["status"] == "ACTIVE")
df3 = df2.withColumn("revenue", df2["price"] * df2["qty"])
```

**Nothing runs yet.** Spark just builds a **query plan** (a recipe). The actual computation only happens when you call an **action** like `.show()`, `.count()`, or `.write`.

> 💡 **Why?** Lazy evaluation lets Spark's optimizer (Catalyst) look at your _entire_ transformation chain and reorder/optimize steps before running anything. This can make your code dramatically faster without you doing anything.
>
> **Analogy**: Instead of following GPS directions turn by turn as you plan a trip, you give your GPS the full destination and it optimizes the _entire_ route before you leave.

---

## 🧱 Two Types of Operations

| Type               | What It Does                                 | Example                               | Runs Immediately? |
| ------------------ | -------------------------------------------- | ------------------------------------- | ----------------- |
| **Transformation** | Creates a new DataFrame from an existing one | `filter`, `select`, `groupBy`, `join` | ❌ No (lazy)      |
| **Action**         | Triggers computation, returns a result       | `show`, `count`, `collect`, `write`   | ✅ Yes            |

---

## 💻 Example 1: Seeing Lazy Evaluation in Action

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

data = [(i, f"user_{i}", i * 100) for i in range(1, 1000001)]
df = spark.createDataFrame(data, ["id", "name", "amount"])

# ---- Transformations: INSTANT (no data processed) ----
import time

t = time.time()
df_filtered   = df.filter(col("amount") > 50000)   # Instant!
df_doubled    = df_filtered.withColumn("double_amt", col("amount") * 2)  # Instant!
df_sorted     = df_doubled.orderBy("double_amt", ascending=False)  # Instant!
print(f"Transformations defined in: {time.time()-t:.4f}s")  # ~0.001s

# ---- Action: TRIGGERS COMPUTATION ----
t = time.time()
row_count = df_sorted.count()   # NOW Spark runs all the transformations!
print(f"count() took: {time.time()-t:.2f}s")   # ~1-3s for real computation
print(f"Filtered rows: {row_count:,}")

# ---- View the execution plan (what Spark will do) ----
df_sorted.explain(verbose=False)
# Shows Physical Plan: how Spark will execute the query
```

---

## 💻 Example 2: Common Transformations Reference

```python
from pyspark.sql.functions import col, upper, round as spark_round

# Sample DataFrame
df = spark.createDataFrame([
    ("ORD-001", 101, "Widget", 5,   199.99, "COMPLETE"),
    ("ORD-002", 102, "Gadget", 2,   499.99, "PENDING"),
    ("ORD-003", 101, "Widget", 10,  199.99, "COMPLETE"),
], ["order_id","customer_id","product","qty","unit_price","status"])

# ---- select: choose/compute columns ----
df.select("order_id", "status").show()
df.select(col("order_id"), (col("qty") * col("unit_price")).alias("revenue")).show()

# ---- filter / where: keep matching rows ----
df.filter(col("status") == "COMPLETE").show()
df.filter((col("qty") > 3) & (col("unit_price") < 300)).show()

# ---- withColumn: add or replace a column ----
df = df.withColumn("revenue", col("qty") * col("unit_price"))
df = df.withColumn("product_upper", upper(col("product")))
df = df.withColumn("revenue_rounded", spark_round(col("revenue"), 2))

# ---- drop: remove columns ----
df = df.drop("product_upper")

# ---- distinct / dropDuplicates ----
df.distinct().show()
df.dropDuplicates(["customer_id", "product"]).show()

# ---- sort / orderBy ----
df.orderBy(col("revenue").desc()).show()
df.orderBy(["status", col("revenue").desc()]).show()
```

---

## 💻 Example 3: Common Actions Reference

```python
# ---- show(): Display rows in console ----
df.show(5)                # First 5 rows, truncated
df.show(5, truncate=False)  # Full values
df.show(5, vertical=True)   # One column per line (nice for many cols)

# ---- count(): Number of rows ----
print(df.count())   # Triggers full scan!

# ---- collect(): Bring all data to driver ----
rows = df.collect()  # ⚠️ ONLY use for small DataFrames!
for row in rows:
    print(row["order_id"], row["revenue"])

# ---- take(n) / first() ----
first_3 = df.take(3)   # List of Row objects
first   = df.first()    # Just the first row

# ---- toPandas(): Convert to Pandas DataFrame ----
pd_df = df.toPandas()  # ⚠️ Collects EVERYTHING to driver — only for small DFs!

# ---- write (the most important action in ETL!) ----
df.write.mode("overwrite").parquet("output/sales_clean/")
```

---

## 💻 Example 4: Caching — Reuse Intermediate Results

If you use an intermediate DataFrame multiple times, **cache it** to avoid recomputing it:

```python
from pyspark.sql.functions import col

# Without caching: df_clean is computed TWICE (once for count, once for write)
df_clean = df.filter(col("status") == "COMPLETE").dropDuplicates(["order_id"])
count = df_clean.count()          # Spark scans + filters — computation 1
df_clean.write.parquet("output/") # Spark scans + filters AGAIN — computation 2!

# With caching: df_clean is computed ONCE and stored in memory
df_clean = df.filter(col("status") == "COMPLETE").dropDuplicates(["order_id"])
df_clean.cache()                   # Register for caching (lazy!)

count = df_clean.count()           # First action: compute + cache in memory
df_clean.write.parquet("output/")  # Uses cached data — much faster!

# Always unpersist when done to free memory
df_clean.unpersist()
```

---

## 💻 Example 5: `explain()` — Read the Execution Plan

```python
# explain() shows you what Spark will actually do (useful for optimization)
df_result = (
    df
    .filter(col("status") == "COMPLETE")
    .groupBy("customer_id")
    .sum("revenue")
)

df_result.explain()
# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=false
# +- HashAggregate(keys=[customer_id], functions=[sum(revenue)])
#    +- Exchange hashpartitioning(customer_id, 4)
#       +- HashAggregate(keys=[customer_id], functions=[partial_sum(revenue)])
#          +- Project [customer_id, revenue]
#             +- Filter (status = COMPLETE)
#                +- Scan csv [order_id, customer_id, qty, unit_price, status, revenue]
# Read bottom-up: Scan → Filter → Project → Aggregate
```

---

## 🏭 ETL Use Cases

| Pattern                                           | Use Case                                  |
| ------------------------------------------------- | ----------------------------------------- |
| Build transformation chain, then ONE write action | Full ETL pipeline                         |
| `.cache()` before multiple reads                  | Reuse filtered/joined intermediate result |
| `.explain()`                                      | Debug slow queries                        |
| `.count()` early                                  | Validate row counts at each stage         |
| Avoid `.toPandas()` on large DFs                  | Only use for final small summary results  |

---

## 🧠 Must-Remember Points

| Rule                            | Remember                                        |
| ------------------------------- | ----------------------------------------------- |
| Nothing runs until an action    | Transformations are lazy — just a recipe        |
| Every action = a Spark job      | Check the Spark UI (localhost:4040) per action  |
| Cache intermediate DFs          | Only when used more than once                   |
| `explain()` is free             | Use it to understand and optimize your pipeline |
| Avoid `collect()` on large data | Brings everything to driver → OOM               |
