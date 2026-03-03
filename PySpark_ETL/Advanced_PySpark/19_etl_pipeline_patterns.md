# 🏭 ETL Pipeline Design Patterns in PySpark (ETL Context)

---

## 🤔 What Are ETL Design Patterns?

Patterns are proven, reusable solutions to common pipeline engineering problems. Instead of reinventing the wheel for each project, you follow established structures that are:

- **Reliable**: Handles failures gracefully
- **Idempotent**: Safe to re-run without duplicating data
- **Observable**: Easy to monitor and debug
- **Modular**: Each step is independently testable

---

## 💻 Pattern 1: Incremental Load (Watermark Pattern)

Only process new/changed records since the last run:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, current_timestamp
from delta.tables import DeltaTable
import json
from pathlib import Path

spark = SparkSession.builder.appName("IncrementalETL").master("local[*]").getOrCreate()

WATERMARK_FILE = "/tmp/etl_watermark.json"

def get_last_watermark() -> str:
    """Load the last successfully processed timestamp."""
    if Path(WATERMARK_FILE).exists():
        return json.loads(Path(WATERMARK_FILE).read_text())["last_run"]
    return "1900-01-01 00:00:00"   # First run: process everything

def save_watermark(ts: str) -> None:
    """Persist the new watermark so next run knows where to start."""
    Path(WATERMARK_FILE).write_text(json.dumps({"last_run": ts}))

# EXTRACT: only new records since last run
last_run = get_last_watermark()
df_new = (
    spark.read.parquet("source/sales/")
    .filter(col("updated_at") > last_run)
)
print(f"New records since {last_run}: {df_new.count():,}")

if df_new.count() == 0:
    print("No new data — skipping run.")
else:
    # TRANSFORM
    df_clean = df_new \
        .filter(col("amount") > 0) \
        .dropDuplicates(["order_id"])

    # LOAD — upsert to Delta
    delta = DeltaTable.forPath(spark, "/delta/sales_clean/")
    delta.alias("t").merge(
        df_clean.alias("s"), "t.order_id = s.order_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    # Save new watermark
    new_ts = df_new.agg(spark_max("updated_at")).first()[0]
    save_watermark(str(new_ts))
    print(f"✅ Load complete. New watermark: {new_ts}")
```

---

## 💻 Pattern 2: Idempotent Write (Safe Re-Runs)

```python
from pyspark.sql.functions import col, year, month

def run_etl(run_date: str) -> None:
    """
    Idempotent: running this function multiple times for the same date
    produces exactly the same result — no duplicates.
    """
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    # EXTRACT for specific date
    df = spark.read.parquet("source/transactions/") \
              .filter(col("tx_date") == run_date)

    # TRANSFORM
    df_clean = (
        df.filter(col("amount") > 0)
          .dropDuplicates(["tx_id"])
          .withColumn("year",  year(col("tx_date")))
          .withColumn("month", month(col("tx_date")))
    )

    # LOAD: overwrite ONLY the specific partition for this date
    # Running twice for "2024-01-15" will overwrite itself — idempotent!
    df_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"tx_date = '{run_date}'") \
        .save("/delta/transactions/")

    print(f"✅ {run_date}: {df_clean.count():,} rows written")

run_etl("2024-01-15")
run_etl("2024-01-15")  # Safe to run again — produces same result!
```

---

## 💻 Pattern 3: Data Quality Validation Layer

```python
from dataclasses import dataclass, field
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan

@dataclass
class DQResult:
    check: str
    passed: bool
    details: str

def run_data_quality_checks(df: DataFrame, job_name: str) -> list[DQResult]:
    """Run a standard set of DQ checks and return pass/fail results."""
    results = []
    total = df.count()

    checks = [
        ("No empty dataset",    lambda: total > 0,
         f"{total} rows"),
        ("No null order_id",    lambda: df.filter(col("order_id").isNull()).count() == 0,
         f"{df.filter(col('order_id').isNull()).count()} null order IDs"),
        ("Positive amounts",    lambda: df.filter(col("amount") <= 0).count() == 0,
         f"{df.filter(col('amount') <= 0).count()} non-positive amounts"),
        ("No NaN in amount",    lambda: df.filter(isnan(col("amount"))).count() == 0,
         "NaN check"),
    ]
    for name, check_fn, details in checks:
        passed = check_fn()
        results.append(DQResult(name, passed, details))
        icon = "✅" if passed else "❌"
        print(f"  {icon} [{job_name}] {name}: {details}")

    failed = [r for r in results if not r.passed]
    if failed:
        raise ValueError(f"Data quality failed: {[r.check for r in failed]}")

    return results

# Usage in pipeline
df_clean = (spark.read.parquet("source/") .filter(col("status") == "COMPLETE"))
run_data_quality_checks(df_clean, "daily_sales_etl")
df_clean.write.mode("overwrite").parquet("output/")
```

---

## 💻 Pattern 4: Modular Pipeline — ETL as Functions

```python
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, upper, current_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etl.sales")

def extract(spark: SparkSession, source_path: str, run_date: str) -> DataFrame:
    logger.info(f"Extracting from {source_path} for {run_date}")
    df = spark.read.parquet(source_path).filter(col("tx_date") == run_date)
    logger.info(f"Extracted: {df.count():,} rows")
    return df

def transform(df: DataFrame) -> DataFrame:
    logger.info("Transforming...")
    df = (df
          .filter(col("amount") > 0)
          .dropDuplicates(["order_id"])
          .withColumn("region", trim(upper(col("region"))))
          .withColumn("etl_ts", current_timestamp()))
    logger.info(f"After transform: {df.count():,} rows")
    return df

def load(df: DataFrame, target_path: str, run_date: str) -> None:
    logger.info(f"Loading to {target_path}")
    df.write.format("delta").mode("overwrite") \
      .option("replaceWhere", f"tx_date = '{run_date}'").save(target_path)
    logger.info("✅ Load complete")

def run_pipeline(run_date: str) -> None:
    spark = SparkSession.builder.appName("sales_etl").getOrCreate()
    df_raw   = extract(spark, "source/sales/", run_date)
    df_clean = transform(df_raw)
    load(df_clean, "/delta/sales/", run_date)

if __name__ == "__main__":
    import sys
    run_pipeline(sys.argv[1])   # python pipeline.py 2024-01-15
```

---

## 🏭 ETL Patterns Summary

| Pattern                         | Problem It Solves                                    |
| ------------------------------- | ---------------------------------------------------- |
| Watermark / incremental         | Process only new data — avoid full table scans       |
| `replaceWhere` idempotent write | Safe re-runs — no duplicate data on retry            |
| DQ validation layer             | Catch bad data before loading to warehouse           |
| Modular extract/transform/load  | Testable, maintainable pipeline code                 |
| Delta MERGE                     | Update existing + insert new in one atomic operation |
