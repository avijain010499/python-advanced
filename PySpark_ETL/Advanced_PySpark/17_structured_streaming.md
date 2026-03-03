# 🌊 Structured Streaming in PySpark (ETL Context)

---

## 🤔 What Is Structured Streaming?

**Structured Streaming** lets you process **continuously arriving data** (real-time events) using the same DataFrame API as batch processing. Spark treats the stream as an unbounded table that keeps growing.

**Common sources**: Kafka, Kinesis, Azure Event Hubs, file folders  
**Common sinks**: Delta Lake, databases, Kafka, dashboards

> 💡 **Analogy**: Batch processing is like washing a pile of dishes at the end of the day. Structured Streaming is like washing each dish the moment you finish using it — continuously, without waiting for a pile to accumulate.

---

## 🧱 Streaming vs Batch

| Feature | Batch                   | Structured Streaming              |
| ------- | ----------------------- | --------------------------------- |
| Data    | Fixed, complete dataset | Unbounded, continuously arriving  |
| Trigger | Manual / scheduled      | Auto (micro-batch or continuous)  |
| API     | Same DataFrame API      | Same DataFrame API + output modes |
| Latency | Minutes-hours           | Seconds (micro-batch)             |

---

## 💻 Example 1: File-Based Streaming (Easiest for Learning)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder.appName("StreamingETL").master("local[*]").getOrCreate()

# Define schema (REQUIRED for streaming — cannot inferSchema!)
schema = StructType([
    StructField("order_id",   StringType(), True),
    StructField("customer_id",StringType(), True),
    StructField("amount",     DoubleType(), True),
    StructField("status",     StringType(), True),
])

# Read streaming: watches a folder for new CSV files
df_stream = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)   # Process 1 new file at a time
    .csv("/tmp/streaming_input/")       # Any new file dropped here is processed!
)

# Transform (same API as batch!)
df_clean = df_stream \
    .filter(col("status") == "COMPLETE") \
    .withColumn("revenue", col("amount")) \
    .withColumn("processed_at", current_timestamp())

# Write stream to output sink
query = (
    df_clean.writeStream
    .outputMode("append")              # append: only new rows written each trigger
    .format("parquet")
    .option("path", "/tmp/streaming_output/")
    .option("checkpointLocation", "/tmp/checkpoints/orders/")  # REQUIRED for recovery!
    .trigger(processingTime="10 seconds")  # Run micro-batch every 10s
    .start()
)

query.awaitTermination()   # Block until query is stopped
```

---

## 💻 Example 2: Kafka Source (Production ETL)

```python
# Requires: spark.jars.packages with kafka connector
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "sales_events")          # Topic name
    .option("startingOffsets", "latest")           # "latest" or "earliest"
    .option("failOnDataLoss", "false")
    .load()
)

# Kafka data comes as binary key/value — decode and parse JSON
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

event_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("amount",   DoubleType(), True),
    StructField("region",   StringType(), True),
])

df_events = (
    df_kafka
    .select(col("value").cast("string").alias("json_str"))
    .select(from_json(col("json_str"), event_schema).alias("data"))
    .select("data.*")   # Explode struct into columns
)

# Write to Delta Lake sink
query = (
    df_events.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/kafka_sales/")
    .table("streaming_sales")
    .start()
)
```

---

## 💻 Example 3: Windowed Aggregations

```python
from pyspark.sql.functions import window, col, sum as spark_sum

# Aggregate events within 5-minute tumbling windows
df_windowed = (
    df_events
    .withWatermark("event_time", "10 minutes")   # Late data tolerance
    .groupBy(
        window(col("event_time"), "5 minutes"),  # 5-minute window
        col("region")
    )
    .agg(spark_sum("amount").alias("window_revenue"))
)

query = (
    df_windowed.writeStream
    .outputMode("update")   # Only update changed aggregates (good for windowed agg)
    .format("console")
    .start()
)
```

---

## 💻 Example 4: Foreach Batch — Custom Sink Logic

```python
def process_batch(batch_df, batch_id):
    """
    Called for each micro-batch. Run any batch operation here:
    complex joins, MERGE into Delta, write to multiple sinks, etc.
    """
    if batch_df.count() == 0:
        return

    print(f"Batch {batch_id}: {batch_df.count()} rows")

    # Enrich with a dimension (static join in streaming context)
    dim = spark.read.parquet("dim/regions/")
    enriched = batch_df.join(dim, on="region", how="left")

    # Upsert to Delta
    from delta.tables import DeltaTable
    delta = DeltaTable.forPath(spark, "/delta/sales_stream/")
    delta.alias("t").merge(
        enriched.alias("s"),
        "t.order_id = s.order_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

query = (
    df_clean.writeStream
    .outputMode("update")
    .foreachBatch(process_batch)    # Call our function for each batch
    .option("checkpointLocation", "/checkpoints/")
    .trigger(processingTime="30 seconds")
    .start()
)
query.awaitTermination()
```

---

## 🏭 ETL Use Cases Summary

| Pattern            | Use Case                                                       |
| ------------------ | -------------------------------------------------------------- |
| File-based stream  | Watch S3/ADLS folder for new files — easy migration from batch |
| Kafka source       | High-throughput event stream (clickstream, IoT)                |
| Delta sink         | Streaming upserts to analytical table                          |
| `.withWatermark()` | Handle late-arriving events gracefully                         |
| `foreachBatch`     | Complex per-batch logic (joins, MERGE, multi-sink)             |

---

## ⚠️ Common Mistakes

```python
# ❌ No checkpointLocation — can't recover on restart!
df.writeStream.format("delta").start("/output/")

# ✅ Always set checkpoint
df.writeStream.format("delta") \
  .option("checkpointLocation", "/checkpoints/my_stream/") \
  .start("/output/")

# ❌ Static join inside readStream — fails!
# (Static df inside streaming query can cause issues without foreachBatch)

# ✅ Use foreachBatch for static joins in streaming
def process(batch_df, id):
    static = spark.read.parquet("dim_table/")
    batch_df.join(static, on="key").write.mode("append").parquet("output/")

df.writeStream.foreachBatch(process).start()
```
