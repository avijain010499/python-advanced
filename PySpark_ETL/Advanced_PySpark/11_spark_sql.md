# 🗄️ Spark SQL in PySpark (ETL Context)

---

## 🤔 What Is Spark SQL?

Spark SQL lets you query DataFrames using **standard SQL syntax** — SELECT, WHERE, GROUP BY, JOIN — all running on the distributed Spark engine.

This is powerful for ETL because:

- SQL is readable and well-understood by data engineers and analysts
- Spark SQL queries are optimized by the same Catalyst engine as DataFrame API
- You can mix SQL and DataFrame code freely in the same pipeline

---

## 💻 Example 1: Running SQL on DataFrames

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

orders = spark.createDataFrame([
    ("ORD-001", 101, "Widget", 5,  199.99, "COMPLETE"),
    ("ORD-002", 102, "Gadget", 2,  499.99, "PENDING"),
    ("ORD-003", 101, "Widget", 10, 199.99, "COMPLETE"),
], ["order_id","customer_id","product","qty","unit_price","status"])

# Step 1: Register DataFrame as a temporary view
orders.createOrReplaceTempView("orders")
# "orders" is now available as a table name in SQL — only for this session!

# Step 2: Run SQL
result = spark.sql("""
    SELECT
        customer_id,
        product,
        SUM(qty * unit_price) AS total_revenue,
        COUNT(*)              AS order_count
    FROM orders
    WHERE status = 'COMPLETE'
    GROUP BY customer_id, product
    ORDER BY total_revenue DESC
""")
result.show()

# Global temp view: survives across different SparkSessions in the same app
orders.createOrReplaceGlobalTempView("orders_global")
spark.sql("SELECT * FROM global_temp.orders_global").show()
```

---

## 💻 Example 2: SQL vs DataFrame API — They're Equivalent

```python
# ---- SQL approach ----
result_sql = spark.sql("""
    SELECT
        region,
        SUM(qty * unit_price) AS revenue,
        COUNT(DISTINCT order_id) AS orders
    FROM orders
    WHERE status = 'COMPLETE'
    GROUP BY region
    HAVING SUM(qty * unit_price) > 1000
    ORDER BY revenue DESC
""")

# ---- DataFrame API equivalent ----
from pyspark.sql.functions import sum as spark_sum, countDistinct, col

result_df = (
    orders
    .filter(col("status") == "COMPLETE")
    .groupBy("region")
    .agg(
        spark_sum(col("qty") * col("unit_price")).alias("revenue"),
        countDistinct("order_id").alias("orders")
    )
    .filter(col("revenue") > 1000)
    .orderBy(col("revenue").desc())
)

# Both produce IDENTICAL results and have the SAME performance!
# Choose based on readability for your team.
```

---

## 💻 Example 3: Multi-Table SQL Join

```python
# Register all tables as views
orders.createOrReplaceTempView("orders")
customers.createOrReplaceTempView("customers")
products.createOrReplaceTempView("products")

enriched = spark.sql("""
    SELECT
        o.order_id,
        o.order_date,
        c.name             AS customer_name,
        c.region,
        p.product_name,
        p.category,
        o.qty,
        o.qty * p.price    AS revenue
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN products  p ON o.product_id  = p.product_id
    WHERE o.status = 'COMPLETE'
      AND o.order_date >= '2024-01-01'
""")
enriched.show()
```

---

## 💻 Example 4: Spark SQL Window Functions

```python
# Window functions work in Spark SQL exactly like in standard SQL
enriched.createOrReplaceTempView("enriched_orders")

ranked = spark.sql("""
    SELECT
        *,
        RANK()        OVER (PARTITION BY region ORDER BY revenue DESC) AS region_rank,
        SUM(revenue)  OVER (PARTITION BY region)                       AS region_total,
        LAG(revenue)  OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_revenue
    FROM enriched_orders
""")
ranked.show()

# Get top 3 orders per region
top3_per_region = spark.sql("""
    WITH ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) AS rn
        FROM enriched_orders
    )
    SELECT * FROM ranked WHERE rn <= 3
""")
top3_per_region.show()
```

---

## 💻 Example 5: Persistent Tables (Hive Metastore)

```python
# For persistent tables (survive between sessions — stored in Hive Metastore):
# (Requires enableHiveSupport() in SparkSession)

spark.sql("CREATE DATABASE IF NOT EXISTS etl_db")
spark.sql("USE etl_db")

# Write DataFrame as a managed Hive table
df.write.saveAsTable("etl_db.sales_clean")

# Or as DDL + CTAS
spark.sql("""
    CREATE TABLE IF NOT EXISTS etl_db.monthly_summary
    USING PARQUET
    PARTITIONED BY (year, month)
    AS
    SELECT
        year(order_date)  AS year,
        month(order_date) AS month,
        region,
        SUM(revenue)      AS total_revenue,
        COUNT(*)          AS order_count
    FROM orders
    GROUP BY 1, 2, 3
""")

# List available tables
spark.sql("SHOW TABLES IN etl_db").show()
spark.sql("DESCRIBE TABLE etl_db.monthly_summary").show()
```

---

## 🏭 ETL Use Cases Summary

| Feature                     | ETL Use Case                                  |
| --------------------------- | --------------------------------------------- |
| `createOrReplaceTempView()` | Run SQL on in-memory DataFrames               |
| Spark SQL JOINs             | Multi-source enrichment in readable SQL       |
| CTEs (`WITH ... AS`)        | Break complex SQL into readable steps         |
| `saveAsTable()`             | Persist results to Hive metastore             |
| SQL window functions        | Ranking, running totals — readable SQL syntax |

---

## ⚠️ Common Mistakes

```python
# ❌ Forgetting to register the view before querying
spark.sql("SELECT * FROM orders")   # AnalysisException: Table not found!

# ✅ Always register first
orders.createOrReplaceTempView("orders")
spark.sql("SELECT * FROM orders").show()

# ❌ Global temp view accessed without "global_temp." prefix
spark.sql("SELECT * FROM my_view")  # Not found — global views need prefix!

# ✅
spark.sql("SELECT * FROM global_temp.my_view")
```
