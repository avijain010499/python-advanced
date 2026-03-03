# 🔥 PySpark Setup & SparkSession (ETL Context)

---

## 🤔 What Is PySpark?

**Apache Spark** is a distributed computing engine designed to process **massive datasets** across a cluster of machines. **PySpark** is its Python API.

Think of it this way:

- **Pandas** processes data on **one machine** — great up to ~10GB
- **PySpark** processes data across **many machines at once** — scales to petabytes

In modern data engineering, PySpark is the backbone of most large-scale ETL pipelines (used in Databricks, AWS EMR, Google Dataproc, Azure HDInsight).

> 💡 **Analogy**: If Pandas is a single powerful cashier, PySpark is a whole team of cashiers working in parallel on different sections of the queue.

---

## 🧱 Key Concepts Before You Start

| Concept             | What It Means                                                  |
| ------------------- | -------------------------------------------------------------- |
| **Cluster**         | A group of machines (nodes) working together                   |
| **Driver**          | The main machine that coordinates the work                     |
| **Executor**        | Worker machines that actually run the computation              |
| **SparkSession**    | The entry point to all PySpark functionality                   |
| **DataFrame**       | Distributed table of data (like Pandas but across the cluster) |
| **Lazy Evaluation** | Nothing runs until you call an _action_ (more in file 04)      |

---

## 🧱 Installation (Local Mode)

```bash
# Install PySpark locally for learning/development
pip install pyspark

# Optional: for Delta Lake support
pip install delta-spark

# Verify installation
python -c "import pyspark; print(pyspark.__version__)"
```

---

## 💻 Example 1: Creating a SparkSession

`SparkSession` is the **single entry point** to PySpark. You must create one before doing anything.

```python
from pyspark.sql import SparkSession

# ---- Local Mode (your laptop — for learning and dev) ----
spark = SparkSession.builder \
    .appName("ETL_Pipeline") \          # Name shown in Spark UI
    .master("local[*]") \               # local[*] = use ALL available CPU cores
    .config("spark.sql.shuffle.partitions", "4") \  # Lower for local dev (default: 200)
    .getOrCreate()                       # Reuse existing session if one exists

# Verify it's running
print(spark.version)          # e.g., 3.5.0
print(spark.sparkContext.master)  # local[*]

# Access the underlying SparkContext (lower-level API)
sc = spark.sparkContext
print(f"Default parallelism: {sc.defaultParallelism}")  # Number of CPU cores
```

---

## 💻 Example 2: SparkSession for Production (Cluster Mode)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("daily_sales_etl") \
    .config("spark.executor.memory", "4g") \       # Memory per executor
    .config("spark.executor.cores", "2") \          # CPU cores per executor
    .config("spark.sql.shuffle.partitions", "200") \# Good for large datasets
    .config("spark.sql.adaptive.enabled", "true") \ # Auto-optimize queries (Spark 3+)
    .enableHiveSupport() \                          # Enable Hive metastore (if available)
    .getOrCreate()

# Set log level (reduces verbose output during dev)
spark.sparkContext.setLogLevel("WARN")   # Options: ALL, DEBUG, INFO, WARN, ERROR, OFF

print("✅ Spark session ready!")
```

---

## 💻 Example 3: Creating Your First DataFrame

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()

# ---- Method 1: From Python list (for testing/dev) ----
data = [
    (1, "Alice", "IT",  75000.0),
    (2, "Bob",   "HR",  60000.0),
    (3, "Charlie","FIN",55000.0),
]
columns = ["emp_id", "name", "dept", "salary"]
df = spark.createDataFrame(data, schema=columns)
df.show()
# +------+-------+----+-------+
# |emp_id|   name|dept| salary|
# +------+-------+----+-------+
# |     1|  Alice|  IT|75000.0|
# |     2|    Bob|  HR|60000.0|
# |     3|Charlie| FIN|55000.0|
# +------+-------+----+-------+

# ---- Method 2: Explicit schema (better for ETL — you control the types) ----
schema = StructType([
    StructField("emp_id", IntegerType(), nullable=False),
    StructField("name",   StringType(),  nullable=True),
    StructField("dept",   StringType(),  nullable=True),
    StructField("salary", FloatType(),   nullable=True),
])
df_typed = spark.createDataFrame(data, schema=schema)
df_typed.printSchema()
# root
#  |-- emp_id: integer (nullable = false)
#  |-- name: string (nullable = true)
#  |-- dept: string (nullable = true)
#  |-- salary: float (nullable = true)
```

---

## 💻 Example 4: Spark UI — Monitor Your Jobs

```python
# The Spark UI runs at http://localhost:4040 while your session is active
# It shows:
# - Jobs: Each action (show, count, write) creates a job
# - Stages: Each job is split into stages (shuffle boundaries)
# - Tasks: Each stage has N tasks (one per partition/core)
# - Storage: What data is cached in memory

# ---- Always stop the session when done! ----
spark.stop()
print("Session stopped")

# Use getOrCreate() pattern so notebooks/scripts are rerunnable without errors
spark = SparkSession.builder.appName("ETL").master("local[*]").getOrCreate()
# If a session already exists → reuses it; if not → creates new one
```

---

## 🏭 ETL Use Cases

| Configuration                     | ETL Use Case                                       |
| --------------------------------- | -------------------------------------------------- |
| `local[*]` master                 | Dev/testing on local machine                       |
| `spark.sql.shuffle.partitions=4`  | Faster local dev (avoid 200 empty partitions)      |
| `spark.sql.adaptive.enabled=true` | Auto-optimize joins/shuffles in production         |
| `getOrCreate()`                   | Safe for notebooks — won't error if session exists |
| `setLogLevel("WARN")`             | Reduce noise in development logs                   |

---

## 🧠 Must-Remember Points

| Rule                                | Remember                                                |
| ----------------------------------- | ------------------------------------------------------- |
| One SparkSession per app            | Use `getOrCreate()` — never create multiple sessions    |
| Stop when done                      | `spark.stop()` releases cluster resources               |
| `local[*]` for dev                  | Uses all CPU cores on your machine                      |
| Shuffle partitions                  | Set to 2-4x number of cores for local; 200+ for cluster |
| SparkContext = `spark.sparkContext` | Lower-level API, accessed via the session               |

---

## ⚠️ Common Mistakes

```python
# ❌ Creating multiple SparkSessions — wastes resources
spark1 = SparkSession.builder.appName("A").getOrCreate()
spark2 = SparkSession.builder.appName("B").getOrCreate()  # They share the same session!

# ✅ Use getOrCreate() everywhere — returns existing session
spark = SparkSession.builder.appName("ETL").getOrCreate()

# ❌ Leaving default shuffle partitions (200) for local dev
# 200 partitions on a 4-core laptop = 196 empty partitions → slow!
# ✅ Set lower for local dev
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
```
