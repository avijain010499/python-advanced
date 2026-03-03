# 🪟 Window Functions in PySpark (ETL Context)

---

## 🤔 What Are Window Functions?

A **window function** computes a value for each row based on a **window** (a group of related rows) — without collapsing the DataFrame like `groupBy`.

Think of it as: "For each row, look at its neighbors and compute something."

> 💡 **Analogy**: Imagine a race. Instead of just finding who won overall (`groupBy`), window functions let you say "For each runner, show their rank **within their age group**" — every runner appears in the result, ranked within their category.

**Common window function types:**

- **Ranking**: `rank()`, `dense_rank()`, `row_number()`
- **Aggregation over window**: `sum()`, `avg()`, `count()` with `over()`
- **Lag/Lead**: Compare a row to previous/next rows

---

## 🧱 Window Specification

```python
from pyspark.sql.window import Window

# Define the window:
window = Window \
    .partitionBy("region") \     # Like GROUP BY — each region is its own window
    .orderBy("revenue")  \       # Order within the window
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)  # Optional frame
```

---

## 💻 Example 1: Ranking Functions

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import rank, dense_rank, row_number, col
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

df = spark.createDataFrame([
    ("Alice","NORTH",5000.0), ("Bob","NORTH",5000.0), ("Charlie","NORTH",3000.0),
    ("Dave","SOUTH",4000.0),  ("Eve","SOUTH",6000.0),  ("Frank","EAST",2000.0),
], ["name","region","revenue"])

# Window: rank within each region by revenue descending
w = Window.partitionBy("region").orderBy(col("revenue").desc())

df_ranked = df \
    .withColumn("rank",       rank().over(w)) \        # Tie → same rank, gap after
    .withColumn("dense_rank", dense_rank().over(w)) \  # Tie → same rank, NO gap
    .withColumn("row_number", row_number().over(w))    # Tie → arbitrary order, no tie

df_ranked.orderBy("region","rank").show()

# RESULT:
# Alice  NORTH  5000  1  1  1
# Bob    NORTH  5000  1  1  2  ← row_number breaks tie arbitrarily
# Charlie NORTH 3000  3  2  3  ← rank has gap(3), dense_rank doesn't(2)
```

---

## 💻 Example 2: ETL Pattern — Top-N Records per Group

```python
from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

# Get the top 2 orders by amount per customer
w = Window.partitionBy("customer_id").orderBy(col("amount").desc())

df_with_rank = df.withColumn("rn", row_number().over(w))

top2_per_customer = df_with_rank.filter(col("rn") <= 2).drop("rn")
top2_per_customer.show()

# Use case: Get the most recent record per customer (SCD Type 1 load pattern)
w_latest = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())
latest_records = (
    df
    .withColumn("rn", row_number().over(w_latest))
    .filter(col("rn") == 1)
    .drop("rn")
)
```

---

## 💻 Example 3: Running Totals and Moving Averages

```python
from pyspark.sql.functions import sum as spark_sum, avg, col
from pyspark.sql.window import Window

# Window: order by date, include all rows from start up to current row
w_running = Window.partitionBy("region") \
    .orderBy("order_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Window: 7-day moving average (3 rows before + current + 3 after)
w_moving = Window.partitionBy("region") \
    .orderBy("order_date") \
    .rowsBetween(-3, 3)

df = df \
    .withColumn("running_total", spark_sum("revenue").over(w_running)) \
    .withColumn("moving_avg_7d", avg("revenue").over(w_moving))

df.show()
```

---

## 💻 Example 4: Lag and Lead — Compare to Previous/Next Row

```python
from pyspark.sql.functions import lag, lead, col, round as r
from pyspark.sql.window import Window

# Window ordered by date per region
w = Window.partitionBy("region").orderBy("order_date")

df = df \
    .withColumn("prev_revenue", lag("revenue", 1).over(w)) \    # Previous row's revenue
    .withColumn("next_revenue", lead("revenue", 1).over(w)) \   # Next row's revenue
    .withColumn("wow_change",                                    # Week-over-week change
        r(((col("revenue") - col("prev_revenue")) / col("prev_revenue") * 100), 2))

df.show()
# Useful for: detecting anomalies, week-over-week reporting, streak detection
```

---

## 💻 Example 5: ETL SCD Type 2 Pattern — Detect Changes

```python
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window

# SCD Type 2: detect when a dimension attribute changes
# e.g., customer changes their region
w = Window.partitionBy("customer_id").orderBy("effective_date")

customer_history = spark.createDataFrame([
    (101,"Alice","NORTH","2023-01-01"),
    (101,"Alice","SOUTH","2024-01-01"),  # Region changed!
    (102,"Bob","EAST","2023-06-01"),
], ["customer_id","name","region","effective_date"])

df_scd = customer_history \
    .withColumn("prev_region", lag("region").over(w)) \
    .withColumn("is_changed",
        col("region") != col("prev_region"))

df_scd.filter(col("is_changed")).show()
# Shows: 101, Alice, SOUTH (changed from NORTH) → triggers SCD Type 2 insert
```

---

## 🏭 ETL Use Cases Summary

| Window Function           | ETL Use Case                                     |
| ------------------------- | ------------------------------------------------ |
| `row_number()`            | Deduplicate (keep latest version of each record) |
| `rank()` / `dense_rank()` | Top-N products per region/category               |
| `sum().over()`            | Running totals for financial reporting           |
| `avg().over(rowsBetween)` | Smoothed KPIs (7-day moving average)             |
| `lag()` / `lead()`        | Week-over-week change, anomaly detection         |

---

## ⚠️ Common Mistakes

```python
# ❌ Forgetting partitionBy — window spans ALL rows (usually wrong!)
w = Window.orderBy("revenue")   # No partitionBy → global window!
df.withColumn("rank", rank().over(w))  # Ranks globally, not per group

# ✅ Almost always partition the window
w = Window.partitionBy("region").orderBy(col("revenue").desc())

# ❌ Using window rank for deduplication without filtering
df.withColumn("rn", row_number().over(w))   # Added column but didn't filter!

# ✅ Filter after adding row number
df.withColumn("rn", row_number().over(w)) \
  .filter(col("rn") == 1) \
  .drop("rn")
```
