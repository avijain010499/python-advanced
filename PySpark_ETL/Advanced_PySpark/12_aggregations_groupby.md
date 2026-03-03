# 📊 Aggregations & GroupBy in PySpark (ETL Context)

---

## 🤔 What Is GroupBy + Aggregation?

GroupBy + aggregation is the most common ETL reporting pattern — take raw transaction-level data and summarize it by categories: revenue by region, orders by month, etc.

PySpark's `.groupBy().agg()` is the equivalent of SQL's `GROUP BY ... SUM/COUNT/AVG`.

---

## 💻 Example 1: Basic GroupBy and Aggregations

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum as spark_sum, count, avg, min, max,
    countDistinct, col, round as spark_round
)

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

df = spark.createDataFrame([
    ("ORD-001","NORTH","Widget",5, 199.99,"COMPLETE"),
    ("ORD-002","SOUTH","Gadget",2, 499.99,"PENDING"),
    ("ORD-003","NORTH","Widget",10,199.99,"COMPLETE"),
    ("ORD-004","NORTH","Gadget",1, 499.99,"COMPLETE"),
    ("ORD-005","SOUTH","Widget",7, 199.99,"COMPLETE"),
], ["order_id","region","product","qty","unit_price","status"])

df = df.withColumn("revenue", col("qty") * col("unit_price"))

# Single-column groupBy
region_summary = df.groupBy("region").agg(
    spark_sum("revenue").alias("total_revenue"),          # Sum
    count("order_id").alias("order_count"),               # Row count
    countDistinct("product").alias("unique_products"),    # Distinct count
    avg("revenue").alias("avg_order_value"),              # Average
    max("revenue").alias("max_order"),                    # Maximum
    min("revenue").alias("min_order"),                    # Minimum
)
region_summary.show()

# Multi-column groupBy
product_region = df.groupBy("region","product").agg(
    spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
    count("*").alias("orders"),
).orderBy("region","total_revenue")
product_region.show()
```

---

## 💻 Example 2: Filter Before and After GroupBy

```python
from pyspark.sql.functions import spark_sum, col

# Filter BEFORE groupBy (reduces data before aggregating — faster!)
df_valid = df.filter(col("status") == "COMPLETE")

result = (
    df_valid
    .groupBy("region")
    .agg(spark_sum("revenue").alias("total_revenue"))
    .filter(col("total_revenue") > 1000)   # HAVING equivalent
    .orderBy(col("total_revenue").desc())
)
result.show()
```

---

## 💻 Example 3: Pivot — Cross-Tab Report

```python
from pyspark.sql.functions import spark_sum, col

# Pivot: turn region values into columns
# Shows revenue per product, broken out by region
pivot_result = df.groupBy("product").pivot("region").agg(
    spark_sum("revenue")
)
# Faster with explicit values (skips one pass to discover unique values):
pivot_fast = df.groupBy("product").pivot("region", ["NORTH","SOUTH","EAST"]).agg(
    spark_sum("revenue")
)
pivot_fast.show()
# +-------+------+------+
# |product| NORTH| SOUTH|
# +-------+------+------+
# | Gadget| 499.9|  null|
# | Widget|2999.9|1399.9|
# +-------+------+------+
```

---

## 💻 Example 4: rollup and cube — Hierarchical Totals

```python
from pyspark.sql.functions import spark_sum, col, coalesce, lit

# rollup: generates subtotals at each level (like ROLLUP in SQL)
rollup_result = df.rollup("region","product").agg(
    spark_sum("revenue").alias("revenue")
).orderBy("region","product")
# Generates: region+product subtotals, region totals, grand total
rollup_result.show()

# cube: generates all combinations of group totals (more complete than rollup)
cube_result = df.cube("region","product").agg(
    spark_sum("revenue").alias("revenue")
).orderBy("region","product")
cube_result.show()
```

---

## 💻 Example 5: Full ETL Monthly Aggregation Pipeline

```python
from pyspark.sql.functions import (
    year, month, col, spark_sum, count, countDistinct,
    avg, round as spark_round, current_timestamp
)

# Extract
df_raw = spark.read.parquet("raw/sales/")

# Transform + Aggregate
monthly_kpis = (
    df_raw
    .filter(col("status") == "COMPLETE")
    .withColumn("revenue", col("qty") * col("unit_price"))
    .withColumn("year",  year(col("order_date")))
    .withColumn("month", month(col("order_date")))
    .groupBy("year", "month", "region")
    .agg(
        spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
        count("order_id").alias("order_count"),
        countDistinct("customer_id").alias("unique_customers"),
        spark_round(avg("revenue"), 2).alias("avg_order_value"),
    )
    .withColumn("etl_ts", current_timestamp())
    .orderBy("year", "month", col("total_revenue").desc())
)

monthly_kpis.show(10)

# Load
monthly_kpis.write \
    .mode("overwrite") \
    .partitionBy("year","month") \
    .parquet("output/monthly_kpis/")

print(f"✅ Monthly KPIs: {monthly_kpis.count():,} rows written")
```

---

## 🏭 ETL Use Cases Summary

| Operation             | Use Case                                        |
| --------------------- | ----------------------------------------------- |
| `.groupBy().agg()`    | Revenue/volume KPIs by dimension                |
| `countDistinct()`     | Unique customers, unique products               |
| `.pivot()`            | Cross-tab reports, wide-format reporting tables |
| `rollup()` / `cube()` | Hierarchical subtotals for BI dashboards        |
| Filter before groupBy | Push down early — reduces shuffle cost          |

---

## ⚠️ Common Mistakes

```python
# ❌ Using count("col") — doesn't count NULLs!
df.groupBy("region").agg(count("amount"))   # Excludes rows where amount is null

# ✅ Use count("*") or count(lit(1)) for all rows
from pyspark.sql.functions import lit
df.groupBy("region").agg(count(lit(1)).alias("total_rows"))

# ❌ HAVING after groupBy requires .filter() not .where() on the column
result = df.groupBy("region").agg(spark_sum("revenue").alias("rev"))
result.where("rev > 1000")   # OK but less readable

# ✅ More explicit
result.filter(col("rev") > 1000)

# ❌ Pivot without specifying values — scans all data first (slow!)
df.groupBy("product").pivot("region").agg(spark_sum("revenue"))

# ✅ Provide explicit pivot values
df.groupBy("product").pivot("region", ["NORTH","SOUTH","EAST"]).agg(spark_sum("revenue"))
```
