# 🏔️ Snowpark — Python DataFrames Inside Snowflake

---

## 🤔 What Is Snowpark?

**Snowpark** lets you write **Python DataFrame code** that runs **inside Snowflake's compute engine** — not on your laptop. The Python code is sent to Snowflake, compiled, and executed using Snowflake's infrastructure.

> 💡 **Think of it this way**: Normal Python ETL = data leaves Snowflake → processed on your server → goes back to Snowflake. **Snowpark = processing stays INSIDE Snowflake** — no data movement, no bandwidth cost, no Python memory limits.

**When to use Snowpark vs Python connector:**

- 🐍 **Connector**: Query Snowflake → bring data to Python → analyze with Pandas
- ❄️ **Snowpark**: Write Python/Pandas-style code that **executes inside Snowflake**

---

## 💻 Example 1: Setting Up Snowpark Session

```python
# pip install snowflake-snowpark-python

from snowflake.snowpark import Session
import os

# Create a Snowpark session (like SparkSession)
connection_params = {
    "account":   os.environ.get("SF_ACCOUNT",  "myorg-account"),
    "user":      os.environ.get("SF_USER",     "ETL_USER"),
    "password":  os.environ.get("SF_PASSWORD", "secret"),
    "warehouse": "TRANSFORM_WH",
    "database":  "ETL_DB",
    "schema":    "STAGING",
    "role":      "TRANSFORMER_ROLE",
}

session = Session.builder.configs(connection_params).create()

# Verify
print(f"Connected: {session.sql('SELECT CURRENT_VERSION()').collect()[0][0]}")
print(f"Database:  {session.get_current_database()}")
print(f"Schema:    {session.get_current_schema()}")
```

---

## 💻 Example 2: Snowpark DataFrame — Same API Feel as PySpark/Pandas

```python
from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import IntegerType, FloatType

# Read a Snowflake table as a Snowpark DataFrame (lazy — nothing happens yet!)
df = session.table("ETL_DB.STAGING.STG_ORDERS")

# Transformations (all compiled to SQL — run in Snowflake, not Python)
df_clean = (
    df
    .filter(F.col("STATUS") == "COMPLETE")       # WHERE STATUS = 'COMPLETE'
    .filter(F.col("AMOUNT") > 0)                  # AND AMOUNT > 0
    .with_column("REVENUE",                        # Derived column
        F.round(F.col("QTY") * F.col("UNIT_PRICE"), 2))
    .with_column("REGION",
        F.upper(F.trim(F.col("REGION"))))
    .with_column("ETL_LOADED_AT", F.current_timestamp())
    .drop_duplicates(["ORDER_ID"])
    .select("ORDER_ID","CUSTOMER_ID","REVENUE","REGION","ORDER_DATE","ETL_LOADED_AT")
)

# Actions — trigger actual execution
print(f"Row count: {df_clean.count():,}")         # Executes Count SQL
df_clean.show(5)                                  # Executes SELECT with LIMIT 5

# Explain the SQL Snowpark will run
df_clean.explain()
```

---

## 💻 Example 3: Write Results Back to Snowflake

```python
# Save back to Snowflake — processed entirely inside Snowflake (no data movement!)
df_clean.write.mode("overwrite").save_as_table("ETL_DB.ANALYTICS.FCT_ORDERS")
# Creates or replaces ETL_DB.ANALYTICS.FCT_ORDERS

# Append (incremental):
df_clean.write.mode("append").save_as_table("ETL_DB.ANALYTICS.FCT_ORDERS")

# Save as a view:
df_clean.create_or_replace_view("ETL_DB.ANALYTICS.V_ORDERS_CLEAN")

print("All transformations ran INSIDE Snowflake — zero data movement!")
```

---

## 💻 Example 4: Snowpark UDFs — Python Code in Snowflake

```python
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import StringType
import re

# Register a Python UDF in Snowflake — runs inside Snowflake on each row
@udf(return_type=StringType(), input_types=[StringType()])
def clean_phone(phone: str) -> str:
    """Remove all non-digit characters from a phone number."""
    if phone is None:
        return None
    return re.sub(r"\D", "", phone)

# Use UDF in DataFrame API
df_customers = session.table("ETL_DB.STAGING.STG_CUSTOMERS")
df_with_clean_phone = df_customers.with_column(
    "PHONE_CLEAN", clean_phone(F.col("PHONE_RAW"))
)
df_with_clean_phone.show(5)

# Register as permanent UDF (reusable in SQL and future Snowpark sessions)
session.udf.register(
    func        = clean_phone,
    name        = "CLEAN_PHONE",
    stage_location = "@ETL_DB.STAGING.PYTHON_STAGE",
    is_permanent = True,
    replace      = True,
)
# Now usable in SQL: SELECT CLEAN_PHONE(PHONE_RAW) FROM STG_CUSTOMERS
```

---

## 💻 Example 5: Vectorized (Pandas) UDF — Faster Batch Processing

```python
from snowflake.snowpark.functions import pandas_udf
from snowflake.snowpark.types import PandasSeriesType, StringType
import pandas as pd

# Vectorized UDF — processes an entire column batch at once (faster than row UDF)
@pandas_udf(return_type=PandasSeriesType(StringType()),
            input_types=[PandasSeriesType(StringType())])
def normalize_region(series: pd.Series) -> pd.Series:
    """Vectorized: normalize region names across the entire column."""
    region_map = {"N": "NORTH", "S": "SOUTH", "E": "EAST", "W": "WEST"}
    return series.str.strip().str.upper().map(
        lambda r: region_map.get(r, r)
    )

df = session.table("STG_ORDERS")
df.with_column("REGION_NORM", normalize_region(F.col("REGION"))).show()
```

---

## 🏭 Snowpark vs SQL — When to Use Which

| Task                        | Snowpark (Python)             | Pure SQL               |
| --------------------------- | ----------------------------- | ---------------------- |
| Complex string parsing      | ✅ Python regex/libraries     | ❌ Limited             |
| ML model inference          | ✅ scikit-learn, statsmodels  | ❌ Not possible        |
| Standard joins/aggregations | Possible, but SQL is simpler  | ✅ Simpler, faster     |
| Reading from external APIs  | ✅ Python requests (in Tasks) | ❌ Not possible        |
| dbt integration             | Use Python models             | ✅ Standard dbt models |

---

## ⚠️ Important Notes

```python
notes = [
    "Snowpark DataFrames are LAZY — no computation until an action (.count(), .show(), .write)",
    "Snowpark UDFs run inside Snowflake — no Python packages available unless uploaded to stage",
    "For standard ETL transforms, pure SQL (via dbt) is often simpler than Snowpark",
    "Snowpark shines for ML scoring, complex string parsing, and library-dependent transforms",
]
for note in notes:
    print(f"📝 {note}")
```
