# 📅 String & Date Functions in PySpark (ETL Context)

---

## 🤔 Why Do You Need These?

Source data arrives with dates as strings in every possible format and strings full of inconsistencies. PySpark's built-in string and date functions let you clean and transform them **at JVM speed** — far faster than Python UDFs.

> 💡 Use `from pyspark.sql.functions import *` to get all the functions below.

---

## 💻 Example 1: String Functions

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, initcap, length,
    regexp_replace, regexp_extract, concat, concat_ws,
    split, substring, lpad, rpad, ltrim, rtrim
)

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

df = spark.createDataFrame([
    ("  ALICE smith  ", "555-123-4567", "$1,500.00", "2024-01-15"),
    ("bob JONES",       "(800)555-9999","$ 200.50",  "15/01/2024"),
    (None,             None,           "3000",       "20240301"),
], ["name","phone","amount_raw","date_raw"])

df = df \
    .withColumn("name_clean",  initcap(trim(col("name")))) \     # "Alice Smith"
    .withColumn("phone_digits", regexp_replace(col("phone"), r"\D", "")) \  # Keep only digits
    .withColumn("amount_clean", regexp_replace(col("amount_raw"), r"[$,\s]","")) \
    .withColumn("first_name",   split(trim(col("name")), r"\s+").getItem(0)) \
    .withColumn("name_length",  length(trim(col("name")))) \
    .withColumn("order_prefix", substring(col("order_id"), 1, 3)) \  # Fixed-width slice
    .withColumn("padded_id",    lpad(col("order_id"), 10, "0"))       # "0000ORD-001"

df.show(truncate=False)
```

---

## 💻 Example 2: Regex Extract — Parse Structured Strings

```python
from pyspark.sql.functions import regexp_extract, col

df = spark.createDataFrame([
    ("Product: Widget_A123 | Region: NORTH | Qty: 50",),
    ("Product: Gadget_B456 | Region: SOUTH | Qty: 20",),
], ["raw"])

df = df \
    .withColumn("product",  regexp_extract(col("raw"), r"Product: (\w+)", 1)) \
    .withColumn("region",   regexp_extract(col("raw"), r"Region: (\w+)", 1)) \
    .withColumn("qty",      regexp_extract(col("raw"), r"Qty: (\d+)", 1).cast("int"))

df.show(truncate=False)
```

---

## 💻 Example 3: Date Parsing and Formatting

```python
from pyspark.sql.functions import (
    to_date, to_timestamp, date_format,
    year, month, dayofmonth, dayofweek, quarter,
    datediff, months_between, add_months, date_add,
    current_date, current_timestamp, col
)

df = spark.createDataFrame([
    ("2024-01-15",    "2024-01-15 10:30:00"),
    ("15/01/2024",    "2024-07-22 14:45:00"),
    ("20240301",      "2024-12-31 23:59:59"),
], ["date_str","ts_str"])

df = df \
    .withColumn("date1",  to_date(col("date_str"), "yyyy-MM-dd")) \
    .withColumn("date2",  to_date(col("date_str"), "dd/MM/yyyy")) \
    .withColumn("date3",  to_date(col("date_str"), "yyyyMMdd")) \
    .withColumn("ts",     to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss"))

# Extract date parts
df = df \
    .withColumn("year",     year(col("date1"))) \
    .withColumn("month",    month(col("date1"))) \
    .withColumn("day",      dayofmonth(col("date1"))) \
    .withColumn("quarter",  quarter(col("date1"))) \
    .withColumn("weekday",  dayofweek(col("date1"))) \        # 1=Sun, 7=Sat
    .withColumn("days_ago", datediff(current_date(), col("date1")))  # Days since event

# Date arithmetic
df = df \
    .withColumn("next_month",    add_months(col("date1"), 1)) \
    .withColumn("due_date",      date_add(col("date1"), 30)) \
    .withColumn("months_old",    months_between(current_date(), col("date1")))

df.show()
```

---

## 💻 Example 4: Date Formatting for Output

```python
from pyspark.sql.functions import date_format, col

df = df.withColumn("formatted_date", date_format(col("date1"), "dd-MMM-yyyy"))
# 2024-01-15 → "15-Jan-2024"

df = df.withColumn("year_month", date_format(col("date1"), "yyyy-MM"))
# 2024-01-15 → "2024-01"

df = df.withColumn("fiscal_label", date_format(col("date1"), "QQQ-yyyy"))
# 2024-01-15 → "Q1-2024"

df.select("date1","formatted_date","year_month","fiscal_label").show()
```

---

## 💻 Example 5: Handle Multiple Date Formats (Common ETL Problem)

```python
from pyspark.sql.functions import coalesce, to_date, col

# Source data has 3 different date formats mixed together
df = spark.createDataFrame([
    ("2024-01-15",),  # ISO
    ("15/01/2024",),  # European
    ("Jan 15, 2024",),# Long
    ("20240301",),    # Compact
], ["raw_date"])

# Try each format; coalesce() takes the first non-null result
df = df.withColumn("parsed_date",
    coalesce(
        to_date(col("raw_date"), "yyyy-MM-dd"),
        to_date(col("raw_date"), "dd/MM/yyyy"),
        to_date(col("raw_date"), "MMM dd, yyyy"),
        to_date(col("raw_date"), "yyyyMMdd"),
    )
)
# Bad formats → null (not a crash!)
unparsed = df.filter(col("parsed_date").isNull()).count()
print(f"Unparseable dates: {unparsed}")
df.show()
```

---

## 🏭 ETL Use Cases Summary

| Function                      | ETL Use Case                           |
| ----------------------------- | -------------------------------------- |
| `trim` / `upper` / `initcap`  | Normalize names, region codes          |
| `regexp_replace`              | Clean currency, phone numbers          |
| `regexp_extract`              | Parse structured fields from free text |
| `to_date(col, format)`        | Parse string dates into date type      |
| `coalesce(to_date(...), ...)` | Handle multiple mixed date formats     |
| `year/month/quarter`          | Build date dimension attributes        |
| `datediff` / `add_months`     | Calculate SLA age, due dates           |

---

## ⚠️ Common Mistakes

```python
# ❌ to_date without format — fails on non-ISO formats
df.withColumn("d", to_date(col("date_str")))    # Works only for yyyy-MM-dd!

# ✅ Always specify format
df.withColumn("d", to_date(col("date_str"), "dd/MM/yyyy"))

# ❌ Using Python UDF for string cleaning — 100x slower
df.withColumn("clean", udf(lambda x: x.strip().upper())(col("name")))

# ✅ Built-in functions
from pyspark.sql.functions import trim, upper
df.withColumn("clean", trim(upper(col("name"))))
```
