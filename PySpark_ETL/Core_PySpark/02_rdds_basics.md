# 📦 RDDs — Resilient Distributed Datasets (ETL Context)

---

## 🤔 What Is an RDD?

An **RDD** is the fundamental data structure of Spark. It is:

- **Resilient**: If a machine fails, Spark can recreate the lost data from its lineage
- **Distributed**: Automatically split across many machines (partitions)
- **Dataset**: A collection of elements (strings, tuples, objects)

> 💡 **Analogy**: Imagine splitting a large book into chapters and giving each chapter to a different person to process simultaneously. Each person works on their chapter (partition) in parallel. RDDs are the chapters.

> ⚠️ **Important**: In modern PySpark ETL, you should almost always use **DataFrames** (file 03). But understanding RDDs helps you understand _why_ Spark works the way it does — and you'll occasionally need them for unstructured data.

---

## 🧱 RDD vs DataFrame

| Feature      | RDD                             | DataFrame                     |
| ------------ | ------------------------------- | ----------------------------- |
| API Level    | Low-level                       | High-level                    |
| Type Safety  | No schema                       | Typed schema                  |
| Optimization | Manual                          | Automatic (Catalyst)          |
| When to Use  | Unstructured data, custom logic | Structured ETL (use this!)    |
| Performance  | Slower (no optimizer)           | Faster (Optimizer + Tungsten) |

---

## 💻 Example 1: Creating RDDs

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD_ETL").master("local[*]").getOrCreate()
sc = spark.sparkContext   # SparkContext is the RDD entry point

# ---- Method 1: Parallelize a Python list ----
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
# Now 'numbers' is distributed across partitions (parallel slices)

print(f"Number of partitions: {numbers.getNumPartitions()}")   # e.g., 4 (= CPU cores)
print(f"Type: {type(numbers)}")   # pyspark.rdd.RDD

# ---- Method 2: From a text file (one element per line) ----
# lines_rdd = sc.textFile("s3://my-bucket/logs/app.log")
# Each line becomes one element in the RDD
```

---

## 💻 Example 2: Common RDD Transformations (Lazy!)

Transformations return a **new RDD** — they don't run immediately:

```python
sc = spark.sparkContext

numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# map(): Apply a function to every element
doubled = numbers.map(lambda x: x * 2)
# [2, 4, 6, 8, 10, 12, 14, 16, 18, 20] — not computed yet!

# filter(): Keep elements where function returns True
evens = numbers.filter(lambda x: x % 2 == 0)
# [2, 4, 6, 8, 10]

# flatMap(): Like map but flattens results (one → many elements)
words = sc.parallelize(["Hello World", "PySpark ETL", "Data Engineering"])
all_words = words.flatMap(lambda sentence: sentence.split(" "))
# ['Hello', 'World', 'PySpark', 'ETL', 'Data', 'Engineering']

# reduceByKey(): Group by key and aggregate values
pairs = sc.parallelize([("IT", 1000), ("HR", 800), ("IT", 1500), ("HR", 600)])
dept_totals = pairs.reduceByKey(lambda a, b: a + b)
# [("IT", 2500), ("HR", 1400)]
```

---

## 💻 Example 3: RDD Actions (Trigger Execution)

Actions **trigger all pending transformations** and return a result:

```python
numbers = sc.parallelize(range(1, 11))  # [1..10]

# collect(): Bring ALL data to driver — ONLY for small results!
all_data = numbers.collect()
print(all_data)          # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# count(): Number of elements
print(numbers.count())   # 10

# first() / take(n)
print(numbers.first())   # 1
print(numbers.take(3))   # [1, 2, 3]

# reduce(): Aggregate all elements to one value
total = numbers.reduce(lambda a, b: a + b)
print(total)             # 55

# saveAsTextFile(): Write to disk (doesn't collect to driver)
# numbers.saveAsTextFile("/tmp/output_rdd/")
```

---

## 💻 Example 4: ETL — Processing Log Files with RDDs

```python
# Logs are often unstructured — RDDs handle them naturally
log_data = sc.parallelize([
    "2024-01-15 10:30:00 INFO  User alice logged in",
    "2024-01-15 10:31:00 ERROR DB connection failed port 5432",
    "2024-01-15 10:32:00 INFO  User bob logged in",
    "2024-01-15 10:33:00 WARN  Slow query: 15s on sales_fact",
    "2024-01-15 10:34:00 ERROR API timeout on /v1/orders",
])

# Step 1: Parse each line into a structured tuple
def parse_log(line):
    parts = line.split(None, 3)  # Split on whitespace, max 4 parts
    if len(parts) == 4:
        date, time, level, message = parts
        return (level, message.strip())
    return ("UNKNOWN", line)

parsed = log_data.map(parse_log)

# Step 2: Filter only ERROR lines
errors = parsed.filter(lambda row: row[0] == "ERROR")

# Step 3: Count errors
error_count = errors.count()
print(f"Total ERROR lines: {error_count}")  # 2

# Step 4: Get error messages
error_messages = errors.map(lambda row: row[1]).collect()
for msg in error_messages:
    print(f"  ❌ {msg}")

# Step 5: Count by log level
level_counts = parsed.map(lambda row: (row[0], 1)) \
                     .reduceByKey(lambda a, b: a + b) \
                     .collect()
print("Log level summary:", dict(level_counts))
# {'INFO': 2, 'ERROR': 2, 'WARN': 1}
```

---

## 💻 Example 5: Convert RDD to DataFrame (Bridge to Modern API)

```python
from pyspark.sql import Row

# Parse raw data into Row objects (named tuples)
raw = sc.parallelize([
    "1,Alice,IT,75000",
    "2,Bob,HR,60000",
    "3,Charlie,FIN,55000",
])

def parse_employee(line):
    parts = line.split(",")
    return Row(
        emp_id = int(parts[0]),
        name   = parts[1].strip(),
        dept   = parts[2].strip(),
        salary = float(parts[3]),
    )

rows_rdd = raw.map(parse_employee)

# Convert to DataFrame (preferred for further processing!)
df = spark.createDataFrame(rows_rdd)
df.show()
df.printSchema()
```

---

## 🏭 ETL Use Cases

| RDD Operation          | ETL Use Case                            |
| ---------------------- | --------------------------------------- |
| `textFile()` + `map()` | Parse unstructured log files            |
| `filter()`             | Isolate error records                   |
| `reduceByKey()`        | Aggregate counts by category            |
| `flatMap()`            | Tokenize / explode nested text          |
| `rdd.toDF()`           | Convert to DataFrame for SQL operations |

---

## ⚠️ Common Mistakes

```python
# ❌ collect() on huge RDDs — brings everything to driver → OOM!
all_data = huge_rdd.collect()   # Millions of rows → crash!

# ✅ Only collect small results; write large results to storage
huge_rdd.saveAsTextFile("s3://bucket/output/")
sample = huge_rdd.take(10)      # Only bring 10 rows to driver

# ❌ Using RDD when DataFrame would be simpler and faster
result = sc.parallelize(rows).map(transform).filter(validate).reduceByKey(agg)

# ✅ Use DataFrame API for structured data
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df.filter(...).groupBy(...).agg(...)
```
