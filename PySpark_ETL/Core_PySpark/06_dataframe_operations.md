# 🛠️ DataFrame Operations — Select, Filter, Join, GroupBy (ETL Context)

---

## 🤔 What Are DataFrame Operations?

Every ETL transformation you need in PySpark maps to a DataFrame operation. This file covers the **core operations** you'll use in every pipeline:

- Selecting and renaming columns
- Filtering rows
- Adding/modifying columns
- Sorting and deduplicating

> 💡 **Key import**: Almost all column operations require `from pyspark.sql.functions import *`. Get comfortable with this module.

---

## 💻 Example 1: Selecting Columns

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws, upper, round as spark_round

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

data = [("ORD-001",101,"Widget",5,199.99,"COMPLETE","NORTH"),
        ("ORD-002",102,"Gadget",2,499.99,"PENDING", "SOUTH"),
        ("ORD-003",101,"Widget",10,199.99,"COMPLETE","EAST")]
df = spark.createDataFrame(data,
     ["order_id","customer_id","product","qty","unit_price","status","region"])

# Select specific columns
df.select("order_id", "status").show()

# Select with expressions
df.select(
    col("order_id"),
    col("product").alias("product_name"),     # Rename
    (col("qty") * col("unit_price")).alias("revenue"),  # Compute
    upper(col("region")).alias("region_upper"),          # Function
    lit("2024").alias("year"),                            # Literal constant
).show()

# Select all except some columns
df.select([c for c in df.columns if c not in ["unit_price", "qty"]]).show()

# selectExpr: use SQL-style strings
df.selectExpr(
    "order_id",
    "qty * unit_price AS revenue",
    "UPPER(region) AS region",
    "status = 'COMPLETE' AS is_complete",   # Boolean column
).show()
```

---

## 💻 Example 2: Filtering Rows

```python
from pyspark.sql.functions import col

# Single condition
df.filter(col("status") == "COMPLETE").show()
df.where(col("status") == "COMPLETE").show()  # where() is alias for filter()

# Multiple conditions — use & (AND) and | (OR), NOT Python 'and'/'or'
df.filter((col("status") == "COMPLETE") & (col("qty") > 3)).show()
df.filter((col("region") == "NORTH") | (col("region") == "EAST")).show()

# isin() — test against a list
df.filter(col("region").isin(["NORTH", "EAST", "SOUTH"])).show()

# isNull / isNotNull
df.filter(col("status").isNull()).show()
df.filter(col("status").isNotNull()).show()

# Negate with ~
df.filter(~col("status").isin(["FAILED", "CANCELLED"])).show()

# String contains / startswith / endswith
df.filter(col("product").contains("Widget")).show()
df.filter(col("order_id").startswith("ORD")).show()
```

---

## 💻 Example 3: Adding and Modifying Columns

```python
from pyspark.sql.functions import col, when, upper, trim, current_timestamp

# withColumn: add new or replace existing column
df = df.withColumn("revenue", col("qty") * col("unit_price"))
df = df.withColumn("region_clean", trim(upper(col("region"))))
df = df.withColumn("etl_loaded_at", current_timestamp())

# when() / otherwise() — SQL CASE WHEN equivalent
df = df.withColumn("tier",
    when(col("revenue") >= 1000, "Gold")
    .when(col("revenue") >= 500,  "Silver")
    .otherwise("Bronze")
)

# withColumnRenamed — rename a column
df = df.withColumnRenamed("customer_id", "cust_id")

# Rename multiple columns at once
rename_map = {"order_id": "order_key", "product": "product_name"}
for old, new in rename_map.items():
    df = df.withColumnRenamed(old, new)

# cast — change data type
from pyspark.sql.types import IntegerType, DoubleType
df = df.withColumn("qty", col("qty").cast(IntegerType()))
df = df.withColumn("revenue", col("revenue").cast(DoubleType()))

# drop — remove columns
df = df.drop("region_clean", "etl_loaded_at")
```

---

## 💻 Example 4: Sorting and Deduplication

```python
from pyspark.sql.functions import col

# Sort ascending (default)
df.orderBy("revenue").show()

# Sort descending
df.orderBy(col("revenue").desc()).show()

# Sort by multiple columns
df.orderBy(["region", col("revenue").desc()]).show()

# dropDuplicates — remove duplicate rows
df.distinct().show()                          # All columns must match
df.dropDuplicates(["customer_id"]).show()       # Deduplicate on one column
df.dropDuplicates(["customer_id","product"]).show()  # Composite key dedup
```

---

## 💻 Example 5: Full ETL Transformation Chain

```python
from pyspark.sql.functions import col, when, upper, trim, current_timestamp, round as r

df_transformed = (
    spark.read.csv("sales_raw.csv", header=True, inferSchema=True)
    # Rename messy source columns
    .withColumnRenamed("ord_id",   "order_id")
    .withColumnRenamed("cst_id",   "customer_id")
    # Clean strings
    .withColumn("region",  trim(upper(col("region"))))
    .withColumn("status",  trim(upper(col("status"))))
    # Compute revenue
    .withColumn("revenue", r(col("qty") * col("unit_price"), 2))
    # Business rule: tier classification
    .withColumn("tier",
        when(col("revenue") >= 5000, "Platinum")
        .when(col("revenue") >= 1000, "Gold")
        .when(col("revenue") >= 500,  "Silver")
        .otherwise("Bronze"))
    # Filter: only valid completed orders
    .filter(col("status") == "COMPLETE")
    .filter(col("revenue") > 0)
    # Dedup
    .dropDuplicates(["order_id"])
    # Add audit column
    .withColumn("etl_loaded_at", current_timestamp())
)

df_transformed.show(5)
df_transformed.write.mode("overwrite").parquet("output/sales_clean/")
print(f"✅ Written {df_transformed.count():,} rows")
```

---

## 🏭 ETL Use Cases Summary

| Operation                   | Use Case                                          |
| --------------------------- | ------------------------------------------------- |
| `select()` / `selectExpr()` | Column projection, type-safe column selection     |
| `filter()`                  | Row-level validity, business rule filtering       |
| `withColumn()`              | Compute revenue, clean strings, add audit columns |
| `when().otherwise()`        | Map codes → labels, tier classification           |
| `dropDuplicates()`          | Idempotent loads — remove re-delivered records    |

---

## ⚠️ Common Mistakes

```python
# ❌ Using Python 'and' / 'or' for conditions — WRONG!
df.filter(col("a") > 0 and col("b") > 0)   # Returns first condition only!

# ✅ Use & and | with parentheses
df.filter((col("a") > 0) & (col("b") > 0))

# ❌ Modifying column in the same withColumn that reads it
df = df.withColumn("revenue", col("qty") * col("unit_price"))
df = df.withColumn("revenue", col("revenue") + col("tax"))   # This is fine!
# But chaining same column in ONE withColumn call causes issues:
df.withColumn("x", col("x") + col("x"))  # OK but reads the original 'x' twice
```
