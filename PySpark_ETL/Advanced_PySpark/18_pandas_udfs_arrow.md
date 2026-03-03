# 🐼 Pandas UDFs & PyArrow in PySpark (ETL Context)

---

## 🤔 What Are Pandas UDFs?

Regular Python UDFs process one row at a time using Python — slow. **Pandas UDFs** process data in **columnar batches (Pandas Series/DataFrame)** using Apache Arrow for zero-copy data transfer between JVM and Python.

**Result**: Pandas UDFs are **10–100x faster** than regular Python UDFs.

> 💡 **When to use**: You need custom Python/NumPy logic that has no built-in Spark equivalent, but you want good performance at scale.

---

## 🧱 Three Types of Pandas UDFs

| Type          | Input              | Output             | Use For                 |
| ------------- | ------------------ | ------------------ | ----------------------- |
| `SCALAR`      | Series per column  | Series             | Row-wise transformation |
| `SCALAR_ITER` | Iterator of Series | Iterator of Series | Expensive model loading |
| `GROUP_MAP`   | Pandas DataFrame   | Pandas DataFrame   | Full group-level logic  |

---

## 💻 Example 1: Scalar Pandas UDF (Most Common)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import FloatType, StringType
import pandas as pd
import re

spark = SparkSession.builder.appName("PandasUDF").master("local[*]").getOrCreate()

df = spark.createDataFrame([
    ("$1,500.00", "  ALICE smith  "),
    ("£ 200.00",  "bob JONES"),
    ("€3,000.50", "Charlie Brown"),
], ["amount_raw","name_raw"])

# Define a Pandas UDF — receives a pd.Series, returns a pd.Series
@pandas_udf(FloatType())
def parse_amount(series: pd.Series) -> pd.Series:
    """Vectorized: operates on an entire column at once."""
    return series.str.replace(r"[$€£,\s]", "", regex=True).astype(float)

@pandas_udf(StringType())
def clean_name(series: pd.Series) -> pd.Series:
    return series.str.strip().str.title()

# Apply like regular column functions
df = df \
    .withColumn("amount", parse_amount(col("amount_raw"))) \
    .withColumn("name",   clean_name(col("name_raw")))

df.show()
```

---

## 💻 Example 2: Iterator Scalar UDF — Load ML Model Once

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import FloatType
from typing import Iterator
import pandas as pd

# Use Iterator variant when you need to load a model/resource once per partition
# (not once per row or once per batch)

@pandas_udf(FloatType())
def predict_churn(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """Load the ML model ONCE per partition, then score many batches."""
    # This runs once per partition (not per batch)
    import joblib
    model = joblib.load("/models/churn_model.pkl")
    print("Model loaded!")   # Appears once per partition in executor logs

    for batch in iterator:
        # batch is a pd.Series of revenue values
        X = batch.values.reshape(-1, 1)
        predictions = model.predict_proba(X)[:, 1]  # Churn probability
        yield pd.Series(predictions, dtype=float)

# df.withColumn("churn_prob", predict_churn(col("revenue")))
```

---

## 💻 Example 3: Grouped Map Pandas UDF — Full Group Logic

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import pandas as pd

# Schema of the OUTPUT DataFrame
result_schema = StructType([
    StructField("region",     StringType(), True),
    StructField("product",    StringType(), True),
    StructField("revenue",    FloatType(),  True),
    StructField("z_score",    FloatType(),  True),   # New column we'll compute
    StructField("is_outlier", IntegerType(), True),
])

@pandas_udf(result_schema)
def detect_outliers(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Run within each group (region+product). Detects revenue outliers using z-score.
    Receives a FULL Pandas DataFrame for the group, returns a FULL Pandas DataFrame.
    """
    mean = pdf["revenue"].mean()
    std  = pdf["revenue"].std()
    pdf["z_score"]    = ((pdf["revenue"] - mean) / std).round(2) if std else 0.0
    pdf["is_outlier"] = (pdf["z_score"].abs() > 2).astype(int)
    return pdf[["region","product","revenue","z_score","is_outlier"]]

df = spark.createDataFrame([
    ("NORTH","Widget",1500.0),("NORTH","Widget",1400.0),
    ("NORTH","Widget",9000.0),("SOUTH","Gadget",800.0),
], ["region","product","revenue"])

result = df.groupby("region","product").applyInPandas(detect_outliers, schema=result_schema)
result.show()
```

---

## 💻 Example 4: toPandas() and createDataFrame() Edge Cases

```python
import pandas as pd

# toPandas(): bring entire Spark DF to driver as Pandas DF
# ⚠️ Only for small DataFrames (final aggregated results)!
summary = df.groupBy("region").sum("revenue")
pd_summary = summary.toPandas()   # OK — summary is small
print(pd_summary.to_markdown())

# createDataFrame(): from Pandas DF to Spark DF
pd_config = pd.DataFrame({
    "region": ["NORTH","SOUTH"],
    "target": [100000, 80000],
})
spark_config = spark.createDataFrame(pd_config)
spark_config.show()

# With Arrow enabled: toPandas / createDataFrame are much faster
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
```

---

## 🏭 ETL Use Cases Summary

| Pandas UDF Type            | ETL Use Case                                          |
| -------------------------- | ----------------------------------------------------- |
| `@pandas_udf(T)` scalar    | Currency parsing, regex cleaning, feature engineering |
| Iterator scalar            | Score with ML model — load model once per partition   |
| Grouped map                | Statistics, outlier detection per group               |
| Arrow-enabled `toPandas()` | Fast conversion of small results to Pandas            |

---

## ⚠️ Common Mistakes

```python
# ❌ Regular Python UDF when Pandas UDF is available
@udf(FloatType())
def slow_parse(s):
    return float(re.sub(r"[$,]","",s)) if s else None  # 1M calls to Python!

# ✅ Pandas UDF — vectorized over entire column
@pandas_udf(FloatType())
def fast_parse(s: pd.Series) -> pd.Series:
    return s.str.replace(r"[$,]","",regex=True).astype(float)  # One call!

# ❌ toPandas on a large DataFrame — OOM!
df.toPandas()   # Attempt to bring 10GB to the driver!

# ✅ Aggregate first, then convert
df.groupBy("region").sum("revenue").toPandas()   # Small result → safe
```
