# 📋 DataFrames & Schemas in PySpark (ETL Context)

---

## 🤔 What Is a PySpark DataFrame?

A PySpark **DataFrame** is a distributed table — like a Pandas DataFrame but spread across many machines. It has:

- **Named columns** with defined data types
- **Schema** — a formal definition of column names and types
- **Lazy evaluation** — transformations build a plan; nothing runs until an action

> 💡 **Why DataFrames over RDDs?** Spark's _Catalyst_ query optimizer and _Tungsten_ execution engine can optimize DataFrame operations automatically — up to 10x faster than equivalent RDD code.

---

## 🧱 PySpark Data Types Quick Reference

```python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType, ArrayType, MapType
)
```

| PySpark Type      | Python Equivalent | Use For            |
| ----------------- | ----------------- | ------------------ |
| `StringType()`    | `str`             | Text, IDs, codes   |
| `IntegerType()`   | `int` (32-bit)    | Small integers     |
| `LongType()`      | `int` (64-bit)    | Large IDs, counts  |
| `DoubleType()`    | `float`           | Financial amounts  |
| `BooleanType()`   | `bool`            | Flags, indicators  |
| `DateType()`      | `date`            | Date without time  |
| `TimestampType()` | `datetime`        | Date with time     |
| `ArrayType(T)`    | `list`            | Array/list columns |
| `MapType(K,V)`    | `dict`            | Key-value columns  |

---

## 💻 Example 1: Reading Data with Schema Inference vs Explicit Schema

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

# ---- Method A: inferSchema (convenient but SLOW for large files) ----
# Spark reads the file TWICE: once to infer types, once to load
df_inferred = spark.read.csv(
    "sales.csv",
    header=True,
    inferSchema=True   # Scans whole file to guess types — slow!
)
df_inferred.printSchema()

# ---- Method B: Explicit Schema (RECOMMENDED for ETL) ----
# Defines types upfront — no extra pass over data, no wrong guesses
schema = StructType([
    StructField("order_id",    StringType(),    nullable=False),
    StructField("customer_id", IntegerType(),   nullable=True),
    StructField("product",     StringType(),    nullable=True),
    StructField("qty",         IntegerType(),   nullable=True),
    StructField("unit_price",  DoubleType(),    nullable=True),
    StructField("order_date",  DateType(),      nullable=True),
    StructField("status",      StringType(),    nullable=True),
])

df = spark.read.csv("sales.csv", header=True, schema=schema)
df.printSchema()
# root
#  |-- order_id: string (nullable = false)
#  |-- customer_id: integer (nullable = true)
#  ...
```

---

## 💻 Example 2: DataFrame Basics

```python
# Create sample DataFrame
data = [
    ("ORD-001", 101, "Widget A", 5,  199.99, "2024-01-15", "COMPLETE"),
    ("ORD-002", 102, "Gadget B", 2,  499.99, "2024-01-16", "PENDING"),
    ("ORD-003", 101, "Widget A", 10, 199.99, "2024-01-17", "COMPLETE"),
    ("ORD-004", 103, "Gadget B", 1,  499.99, "2024-01-18", "FAILED"),
]
cols = ["order_id","customer_id","product","qty","unit_price","order_date","status"]
df = spark.createDataFrame(data, schema=cols)

# ---- Inspect the DataFrame ----
df.show(5)              # Display first 5 rows (triggers action!)
df.show(truncate=False) # Don't truncate long values
df.printSchema()         # Show column names and types
print(df.columns)        # ['order_id', 'customer_id', ...]
print(df.dtypes)         # [('order_id', 'string'), ...]
print(df.count())        # 4  — triggers a full scan!
df.describe("unit_price", "qty").show()  # Basic stats for numeric columns
```

---

## 💻 Example 3: Schema with Nested Types (JSON/API Data)

```python
from pyspark.sql.types import *

# Schema for nested JSON from an API
order_schema = StructType([
    StructField("order_id",   StringType(),   False),
    StructField("order_date", TimestampType(), True),
    StructField("customer", StructType([          # Nested struct
        StructField("id",     IntegerType(), True),
        StructField("name",   StringType(),  True),
        StructField("region", StringType(),  True),
    ]), True),
    StructField("line_items", ArrayType(          # Array of structs
        StructType([
            StructField("product", StringType(), True),
            StructField("qty",     IntegerType(), True),
            StructField("price",   DoubleType(),  True),
        ])
    ), True),
    StructField("tags", MapType(StringType(), StringType()), True),
])

df_json = spark.read.json("orders.json", schema=order_schema)
df_json.printSchema()

# Access nested fields
df_json.select(
    "order_id",
    "customer.name",            # Dot notation for structs
    "customer.region",
    "line_items",               # Whole array
).show()
```

---

## 💻 Example 4: Schema Validation — ETL Best Practice

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def validate_schema(df, required_schema: StructType) -> list:
    """
    Compare actual DataFrame schema to expected schema.
    Returns list of issues — empty list means schema is valid.
    """
    issues = []
    actual_fields = {f.name: f.dataType for f in df.schema.fields}

    for expected_field in required_schema.fields:
        if expected_field.name not in actual_fields:
            issues.append(f"MISSING column: '{expected_field.name}'")
        elif type(actual_fields[expected_field.name]) != type(expected_field.dataType):
            actual_type   = actual_fields[expected_field.name].simpleString()
            expected_type = expected_field.dataType.simpleString()
            issues.append(
                f"TYPE MISMATCH: '{expected_field.name}' is {actual_type}, expected {expected_type}"
            )
    return issues


expected = StructType([
    StructField("order_id",    StringType(),  False),
    StructField("customer_id", IntegerType(), True),
    StructField("amount",      DoubleType(),  True),
])

issues = validate_schema(df, expected)
if issues:
    for issue in issues:
        print(f"❌ Schema Error: {issue}")
    raise ValueError(f"Schema validation failed: {len(issues)} issues")
else:
    print("✅ Schema validation passed!")
```

---

## 🏭 ETL Use Cases

| Pattern                      | Use Case                              |
| ---------------------------- | ------------------------------------- |
| Explicit schema on read      | Always — speed + correctness          |
| `StructType` for nested JSON | API responses, MongoDB exports        |
| `ArrayType`                  | Line items, tags, multi-value columns |
| Schema validation function   | Early catching of source schema drift |
| `df.printSchema()`           | Debug and document data structure     |

---

## ⚠️ Common Mistakes

```python
# ❌ inferSchema on large files — reads the file TWICE (very slow!)
df = spark.read.csv("500gb_file.csv", header=True, inferSchema=True)

# ✅ Define schema explicitly
df = spark.read.csv("500gb_file.csv", header=True, schema=my_schema)

# ❌ Using Pandas dtype after creating PySpark DF
df.astype({"col": int})   # AttributeError — this is Pandas API!

# ✅ Use PySpark casting
from pyspark.sql.functions import col
df = df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
```
