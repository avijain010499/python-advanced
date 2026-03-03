# 📂 Reading & Writing Data in PySpark (ETL Context)

---

## 🤔 Why Is This Core to ETL?

Every ETL pipeline starts with reading data (Extract) and ends with writing data (Load). PySpark supports almost every format used in modern data engineering.

> 💡 **Parquet is king** in big data. Always read/write Parquet when you have a choice — it's columnar, compressed, and 10x faster to read than CSV at scale.

---

## 🧱 The Read/Write Pattern

```python
# READ:  spark.read.<format>(<options>).load("path")
# WRITE: df.write.<options>.<format>("path")

# Shorthand equivalents:
spark.read.parquet("path")           # == spark.read.format("parquet").load("path")
df.write.parquet("path")             # == df.write.format("parquet").save("path")
```

---

## 💻 Example 1: Reading CSV

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

schema = StructType([
    StructField("order_id",    StringType(),  False),
    StructField("customer_id", IntegerType(), True),
    StructField("amount",      DoubleType(),  True),
    StructField("order_date",  DateType(),    True),
    StructField("status",      StringType(),  True),
])

df = spark.read.csv(
    "data/sales/*.csv",   # Read ALL CSVs in the folder (wildcard!)
    header    = True,
    schema    = schema,
    dateFormat= "yyyy-MM-dd",
    nullValue = "N/A",      # Treat "N/A" strings as null
    mode      = "PERMISSIVE",  # PERMISSIVE: bad rows → null | DROPMALFORMED: skip | FAILFAST: crash
)
print(f"Rows loaded: {df.count():,}")
df.show(5)
```

---

## 💻 Example 2: Reading Parquet

```python
# Parquet: reads only requested columns (columnar format!)
df = spark.read.parquet("data/sales/")
df.printSchema()   # Schema embedded in file — no need to specify!

# Read only specific columns (column pruning — much faster!)
df_light = spark.read.parquet("data/sales/").select("order_id", "amount", "status")

# Read a partitioned dataset (Hive-style partitioning)
# e.g., data/sales/year=2024/month=01/part-*.parquet
df_jan = spark.read.parquet("data/sales/year=2024/month=01/")
# Or let Spark discover partitions automatically:
df_all = spark.read.parquet("data/sales/")   # Automatically discovers year, month
# df_all now has columns: order_id, amount, status, year, month
```

---

## 💻 Example 3: Reading JSON

```python
# Single-line JSON (one document per line — JSONL format)
df_jsonl = spark.read.json("data/events.jsonl")

# Multiline JSON (one document spans multiple lines)
df_json = spark.read.option("multiline", "true").json("data/orders.json")

# With schema
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
schema = StructType([
    StructField("id",     StringType(), True),
    StructField("amount", DoubleType(), True),
])
df = spark.read.schema(schema).json("data/orders.jsonl")
```

---

## 💻 Example 4: Writing Data — Write Modes

```python
# Write modes:
# "overwrite" = delete existing data and write fresh
# "append"    = add new data without deleting existing
# "ignore"    = skip if destination already exists
# "error"     = fail if destination exists (default)

# ---- Write Parquet (partitioned by date — for data lakes) ----
df.write \
  .mode("overwrite") \
  .partitionBy("year", "month") \   # Creates year=XXXX/month=XX/ folder structure
  .parquet("output/sales/")

# ---- Write CSV ----
df.write \
  .mode("overwrite") \
  .option("header", "true") \
  .option("dateFormat", "yyyy-MM-dd") \
  .csv("output/sales_csv/")

# ---- Write a single CSV (coalesce to 1 file — ETL delivery) ----
# WARNING: Collects all data to one executor — only for small results!
df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv("output/sales_single_file/")

# ---- Write JSON ----
df.write.mode("overwrite").json("output/sales_json/")
```

---

## 💻 Example 5: Reading from a Database (JDBC)

```python
# ---- Read from PostgreSQL ----
jdbc_url = "jdbc:postgresql://localhost:5432/etl_db"
props    = {"user": "etl_user", "password": "secret", "driver": "org.postgresql.Driver"}

# Read entire table
df_sales = spark.read.jdbc(url=jdbc_url, table="sales", properties=props)

# Read with a query
df_q = spark.read.jdbc(
    url        = jdbc_url,
    table      = "(SELECT * FROM sales WHERE status = 'COMPLETE') AS q",
    properties = props,
)

# Read in parallel using numPartitions (one JDBC connection per partition)
df_parallel = spark.read.jdbc(
    url             = jdbc_url,
    table           = "sales",
    column          = "customer_id",   # Partition by this numeric column
    lowerBound      = 1,
    upperBound      = 10000,
    numPartitions   = 10,              # 10 parallel DB connections!
    properties      = props,
)

# ---- Write to PostgreSQL ----
df_transformed.write.jdbc(
    url        = jdbc_url,
    table      = "sales_transformed",
    mode       = "append",
    properties = props,
)
```

---

## 💻 Example 6: Full ETL Read → Transform → Write

```python
# EXTRACT
df_raw = spark.read.schema(schema).option("mode","PERMISSIVE").csv(
    "s3://raw-bucket/sales/2024/",
    header=True,
)
print(f"Extracted: {df_raw.count():,} rows")

# TRANSFORM
from pyspark.sql.functions import col, upper, current_timestamp

df_clean = (
    df_raw
    .filter(col("amount").isNotNull() & (col("amount") > 0))
    .filter(col("status") == "COMPLETE")
    .withColumn("region", upper(col("region")))
    .withColumn("etl_loaded_at", current_timestamp())
    .dropDuplicates(["order_id"])
)
print(f"After transform: {df_clean.count():,} rows")

# LOAD
df_clean.write \
    .mode("append") \
    .partitionBy("year", "month") \
    .parquet("s3://clean-bucket/sales/")
print("✅ Load complete!")
```

---

## 🏭 ETL Use Cases Summary

| Format  | Read Method            | Write Method       | Notes                            |
| ------- | ---------------------- | ------------------ | -------------------------------- |
| CSV     | `spark.read.csv()`     | `.write.csv()`     | Always specify schema            |
| Parquet | `spark.read.parquet()` | `.write.parquet()` | Preferred for data lakes         |
| JSON    | `spark.read.json()`    | `.write.json()`    | `multiline=true` for pretty JSON |
| JDBC/DB | `spark.read.jdbc()`    | `.write.jdbc()`    | Use `numPartitions` for speed    |

---

## ⚠️ Common Mistakes

```python
# ❌ inferSchema on large CSV — reads entire file twice!
df = spark.read.csv("huge.csv", header=True, inferSchema=True)
# ✅
df = spark.read.csv("huge.csv", header=True, schema=my_schema)

# ❌ Writing without partitionBy on large datasets — one huge folder
df.write.parquet("output/")
# ✅ Partition for efficient downstream reads
df.write.partitionBy("year","month").parquet("output/")

# ❌ coalesce(1) on huge DataFrames — creates a bottleneck!
df.coalesce(1).write.csv("output/")   # Only ONE executor writes!
# ✅ Only use coalesce(1) for small result sets
```
