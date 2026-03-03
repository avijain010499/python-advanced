# 🧩 UDFs — User-Defined Functions in PySpark (ETL Context)

---

## 🤔 What Are UDFs?

PySpark's built-in functions (`upper`, `trim`, `when`, etc.) cover most needs. But sometimes you need **custom business logic** that has no built-in equivalent.

A **UDF (User-Defined Function)** lets you write a Python function and apply it to DataFrame columns.

> ⚠️ **Performance Warning**: Regular Python UDFs are slow — Spark must serialize each row to Python, run your function, and serialize back. Always prefer:
>
> 1. Built-in Spark functions
> 2. Pandas UDFs (vectorized) — see file 18
> 3. Python UDFs (last resort for unstructured logic)

---

## 💻 Example 1: Basic UDF

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

df = spark.createDataFrame([
    ("Alice Smith",  "555-123-4567", "$1,500.00"),
    ("BOB JONES",    "(800) 555-9999","$ 200.50"),
    ("charlie brown","  +1-800-555-0001","3000"),
], ["name", "phone", "amount_raw"])

# Step 1: Define a regular Python function
def title_case(s):
    if s is None:
        return None
    return s.strip().title()

# Step 2: Register as a UDF with the return type
title_case_udf = udf(title_case, StringType())
# StringType() → the RETURN type of the function (not the input type)

# Step 3: Apply to a column
df = df.withColumn("name_clean", title_case_udf(col("name")))
df.show()
```

---

## 💻 Example 2: UDF with Complex Logic

```python
import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType

# Clean currency strings: "$1,500.00" → 1500.0
def parse_amount(raw):
    if raw is None:
        return None
    cleaned = re.sub(r"[$€£,\s]", "", raw)
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None

parse_amount_udf = udf(parse_amount, FloatType())

# Normalize phone numbers
def clean_phone(phone):
    if phone is None:
        return None
    digits = re.sub(r"\D", "", phone)  # Keep only digits
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]  # Remove country code
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return None

clean_phone_udf = udf(clean_phone, StringType())

# Apply both UDFs
df = df \
    .withColumn("amount",      parse_amount_udf(col("amount_raw"))) \
    .withColumn("phone_clean", clean_phone_udf(col("phone")))
df.show()
```

---

## 💻 Example 3: Register UDF for Spark SQL

```python
# register() makes the UDF available in SQL queries
spark.udf.register("title_case", title_case, StringType())
spark.udf.register("parse_amount", parse_amount, FloatType())

# Create a temp view
df.createOrReplaceTempView("raw_records")

# Use UDFs in SQL
result = spark.sql("""
    SELECT
        title_case(name)       AS name_clean,
        parse_amount(amount_raw) AS amount,
        phone
    FROM raw_records
    WHERE parse_amount(amount_raw) > 0
""")
result.show()
```

---

## 💻 Example 4: UDF with Multiple Return Values (StructType)

```python
from pyspark.sql.types import StructType, StructField, StringType

# Return a struct (multiple values) from a single UDF
def parse_full_name(full_name):
    if full_name is None:
        return ("Unknown", "Unknown")
    parts = full_name.strip().split(None, 1)  # Split at first whitespace
    first = parts[0].title() if len(parts) > 0 else "Unknown"
    last  = parts[1].title() if len(parts) > 1 else "Unknown"
    return (first, last)

name_schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name",  StringType(), True),
])

parse_name_udf = udf(parse_full_name, name_schema)

df = df.withColumn("parsed_name", parse_name_udf(col("name")))

# Access the struct fields using dot notation
df = df \
    .withColumn("first_name", col("parsed_name.first_name")) \
    .withColumn("last_name",  col("parsed_name.last_name")) \
    .drop("parsed_name")

df.show()
```

---

## 💻 Example 5: When to Use UDF vs Built-In Functions

```python
from pyspark.sql.functions import upper, trim, regexp_replace, col

# ✅ PREFER: Built-in functions (run in JVM — fast!)
df = df \
    .withColumn("name_clean", trim(upper(col("name")))) \
    .withColumn("amount", regexp_replace(col("amount_raw"), r"[$,\s]", "").cast("double"))

# ⚠️ USE UDF ONLY WHEN: complex conditional logic, external library calls,
#    or operations with no built-in equivalent
@udf(StringType())
def classify_customer(amount, region):
    """Complex multi-input logic — no direct built-in equivalent."""
    if amount is None or region is None:
        return "Unknown"
    if amount > 10000 and region == "NORTH":
        return "VIP_NORTH"
    elif amount > 5000:
        return "Premium"
    return "Standard"

df = df.withColumn("customer_class", classify_customer(col("amount"), col("region")))
```

---

## 🏭 ETL Use Cases Summary

| UDF Type                     | Use Case                                         |
| ---------------------------- | ------------------------------------------------ |
| String cleaning UDF          | Custom phone/address/currency parsing            |
| Struct-returning UDF         | Parse one column into multiple structured fields |
| SQL-registered UDF           | Reuse logic in Spark SQL and Delta views         |
| `@udf(returnType)` decorator | Concise registration for simple functions        |

---

## ⚠️ Performance Guidance

```
Speed order (fastest → slowest):
1. Built-in Spark functions (pyspark.sql.functions)  ← Use these first!
2. Pandas UDF / vectorized UDF (file 18)             ← For custom batch ops
3. Regular Python UDF                                ← Last resort
4. RDD.map() with Python                             ← Avoid in modern PySpark

# ❌ Slow: Python UDF on 1M rows
df.withColumn("clean", clean_udf(col("name")))   # 1M Python function calls!

# ✅ Fast: Equivalent built-in
from pyspark.sql.functions import trim, upper
df.withColumn("clean", trim(upper(col("name"))))  # JVM-level, no Python overhead
```
