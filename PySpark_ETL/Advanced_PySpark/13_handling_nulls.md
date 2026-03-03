# 🕳️ Handling Null Values in PySpark (ETL Context)

---

## 🤔 Why Are Nulls Critical in ETL?

In distributed data, nulls are everywhere — missing join keys, optional fields, failed conversions. Handle them wrong and you get:

- Silent incorrect aggregations (null propagates through most arithmetic)
- Broken joins (null != null in SQL/Spark)
- Pipeline crashes when unexpected nulls reach operations that assume non-null input

> 💡 **Null in Spark**: `null != null`. Any comparison with null returns null (not True or False). Use `.isNull()` and `.isNotNull()`, never `== None`.

---

## 💻 Example 1: Detect Nulls

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, sum as spark_sum

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

df = spark.createDataFrame([
    ("ORD-1", 101, 1500.0, "NORTH"),
    ("ORD-2", None, None,  "SOUTH"),   # Null customer_id and amount
    ("ORD-3", 103, 300.0,  None),      # Null region
    ("ORD-4", 104, float("nan"), "EAST"),  # NaN (different from null!)
], ["order_id","customer_id","amount","region"])

# Count nulls per column
null_counts = df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])
null_counts.show()
# +--------+-----------+------+------+
# |order_id|customer_id|amount|region|
# +--------+-----------+------+------+
# |       0|          1|     1|     1|
# +--------+-----------+------+------+

# NaN (not-a-number) is DIFFERENT from null — check separately
nan_counts = df.select([
    spark_sum(isnan(col(c)).cast("int")).alias(c)
    for c in ["amount"]   # isnan only works on numeric columns
])
nan_counts.show()
```

---

## 💻 Example 2: Drop Rows with Nulls

```python
# Drop rows where ANY column is null
df.dropna().show()                        # All columns must be non-null

# Drop rows where ALL columns are null
df.dropna(how="all").show()

# Drop rows where specific columns are null
df.dropna(subset=["customer_id","amount"]).show()  # Must have both

# Require at least N non-null columns
df.dropna(thresh=3).show()   # Keep rows with ≥ 3 non-null values
```

---

## 💻 Example 3: Fill Nulls with Default Values

```python
# Fill all nulls with a single value (same type required)
df.fillna(0).show()              # Fills ALL numeric nulls with 0
df.fillna("UNKNOWN").show()      # Fills ALL string nulls with "UNKNOWN"

# Fill different columns with different values
df.fillna({
    "customer_id": 0,
    "amount":      0.0,
    "region":      "UNKNOWN",
}).show()

# From pyspark.sql.functions import coalesce — use first non-null across columns
from pyspark.sql.functions import coalesce, lit
df = df.withColumn("region_filled",
    coalesce(col("region"), col("default_region"), lit("GLOBAL"))
)
# Returns first non-null: region → default_region → "GLOBAL"
```

---

## 💻 Example 4: Replace Nulls with Column-Level Aggregates (Imputation)

```python
from pyspark.sql.functions import mean, col, when

# Replace null amounts with the column's average (mean imputation)
avg_amount = df.agg(mean("amount")).first()[0]
print(f"Mean amount: {avg_amount:.2f}")

df = df.withColumn("amount_imputed",
    when(col("amount").isNull(), avg_amount)
    .otherwise(col("amount"))
)

# Or fill null region with the most common (mode) region
mode_region = (
    df.groupBy("region").count()
    .orderBy(col("count").desc())
    .first()["region"]
)
df = df.withColumn("region",
    when(col("region").isNull(), mode_region).otherwise(col("region"))
)
```

---

## 💻 Example 5: Handle NaN (Separate from Null)

```python
from pyspark.sql.functions import isnan, when, col, lit

# NaN is a floating-point concept — isNull() does NOT catch NaN!
df.filter(col("amount").isNull()).show()   # Misses NaN rows!
df.filter(isnan(col("amount"))).show()    # Catches NaN rows

# Replace NaN with null (then handle just nulls from there)
from pyspark.sql.functions import nanvl
df = df.withColumn("amount",
    when(isnan(col("amount")), None)    # NaN → null
    .otherwise(col("amount"))
)
# Now fillna handles everything uniformly
df = df.fillna({"amount": 0.0})
```

---

## 💻 Example 6: Full ETL Null Handling Pattern

```python
from pyspark.sql.functions import col, when, coalesce, lit, isnan, mean

def handle_nulls(df):
    """Standard null-handling block for ETL pipelines."""
    # 1. Turn NaN → null for numeric cols
    for c in ["amount","unit_price","qty"]:
        if c in df.columns:
            df = df.withColumn(c, when(isnan(col(c)), None).otherwise(col(c)))

    # 2. Drop rows missing critical keys
    df = df.dropna(subset=["order_id","customer_id"])

    # 3. Fill optional fields with defaults
    df = df.fillna({
        "region":  "UNKNOWN",
        "status":  "PENDING",
        "amount":  0.0,
    })

    # 4. Log what's left
    remaining = df.filter(col("amount").isNull()).count()
    if remaining:
        print(f"⚠️  {remaining} rows still have null amount after fill")

    return df

df_clean = handle_nulls(df)
df_clean.show()
```

---

## 🏭 ETL Use Cases Summary

| Operation                 | Use Case                                               |
| ------------------------- | ------------------------------------------------------ |
| `dropna(subset=[...])`    | Drop rows missing critical join keys                   |
| `fillna({col: val})`      | Fill optional fields with defaults                     |
| `coalesce()`              | First non-null across multiple fallback columns        |
| `when(isnan(), None)`     | Normalize NaN → null before any processing             |
| Imputation with mean/mode | Replace missing numeric data with statistical estimate |

---

## ⚠️ Common Mistakes

```python
# ❌ Comparing to None — ALWAYS FALSE in Spark
df.filter(col("region") == None)    # Returns empty DF!

# ✅
df.filter(col("region").isNull())

# ❌ Forgetting NaN when checking for "missing" numeric values
df.filter(col("amount").isNull()).count()   # Misses NaN rows!

# ✅ Check both
from pyspark.sql.functions import isnan
df.filter(col("amount").isNull() | isnan(col("amount"))).count()

# ❌ fillna(0) fills ALL columns including IDs!
df.fillna(0)  # fills "customer_id" with 0 too — now fake IDs exist!

# ✅ Always target specific columns
df.fillna({"amount": 0.0, "qty": 0})
```
