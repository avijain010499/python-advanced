# 🐍 Python Connector & Pandas Integration

---

## 🤔 Why Use Python with Snowflake?

Python lets you orchestrate the full ETL loop:

- Read from APIs, databases, or files into Pandas
- Transform with Pandas or Spark
- Load into Snowflake using the connector
- Query results back into Pandas for reporting

---

## 💻 Example 1: snowflake-sqlalchemy — The SQL-First Way

```python
# pip install snowflake-sqlalchemy pandas sqlalchemy

from sqlalchemy import create_engine, text
import pandas as pd, os

def get_engine():
    """Create a SQLAlchemy engine for Snowflake."""
    account  = os.environ.get("SF_ACCOUNT",  "myorg-account")
    user     = os.environ.get("SF_USER",     "ETL_USER")
    password = os.environ.get("SF_PASSWORD", "secret")
    db       = "ETL_DB"
    schema   = "ANALYTICS"
    wh       = "TRANSFORM_WH"

    url = f"snowflake://{user}:{password}@{account}/{db}/{schema}?warehouse={wh}"
    return create_engine(url, echo=False)

# ---- READ: SQL → Pandas ----
engine = get_engine()
df = pd.read_sql(
    "SELECT * FROM FCT_ORDERS WHERE ORDER_DATE >= DATEADD('DAY',-30, CURRENT_DATE())",
    engine,
)
print(f"Loaded {len(df):,} rows")
print(df.dtypes)

# ---- WRITE: Pandas → Snowflake ----
df_summary = df.groupby("REGION")["REVENUE"].sum().reset_index()
df_summary.to_sql(
    name      = "MONTHLY_REGION_SUMMARY",
    con       = engine,
    schema    = "ANALYTICS",
    if_exists = "replace",   # "replace" drops+recreates, "append" adds rows
    index     = False,
    method    ="multi",       # Batch inserts (faster than row-by-row)
    chunksize = 10_000,
)
print(f"Written {len(df_summary)} rows to MONTHLY_REGION_SUMMARY")
```

---

## 💻 Example 2: Native Connector with DictCursor

```python
import snowflake.connector, os
from typing import Any

conn = snowflake.connector.connect(
    account   = os.environ.get("SF_ACCOUNT", "myorg-account"),
    user      = os.environ.get("SF_USER",    "ETL_USER"),
    password  = os.environ.get("SF_PASSWORD","secret"),
    warehouse = "TRANSFORM_WH",
    database  = "ETL_DB",
    schema    = "ANALYTICS",
)

# DictCursor returns rows as dicts (column_name: value)
with conn.cursor(snowflake.connector.DictCursor) as cur:

    # Parameterized query (ALWAYS use % params — prevents SQL injection!)
    cur.execute(
        "SELECT * FROM FCT_ORDERS WHERE REGION = %s AND ORDER_DATE >= %s",
        ("NORTH", "2024-01-01"),
    )
    rows = cur.fetchall()  # Returns list of dicts
    for row in rows[:3]:
        print(row)

    # executemany: batch INSERT (for small data)
    insert_sql = "INSERT INTO REF_REGIONS (CODE, NAME) VALUES (%s, %s)"
    data = [("US","United States"), ("UK","United Kingdom"), ("IN","India")]
    cur.executemany(insert_sql, data)
    conn.commit()  # DML requires commit with native connector

conn.close()
```

---

## 💻 Example 3: write_pandas — Fastest Python Load Method

```python
import pandas as pd, snowflake.connector, os
from snowflake.connector.pandas_tools import write_pandas

conn = snowflake.connector.connect(
    account   = os.environ.get("SF_ACCOUNT",  "myorg-account"),
    user      = os.environ.get("SF_USER",     "ETL_USER"),
    password  = os.environ.get("SF_PASSWORD", "secret"),
    warehouse = "TRANSFORM_WH",
    database  = "ETL_DB",
    schema    = "STAGING",
)

df = pd.DataFrame({
    "ORDER_ID":   ["ORD-001","ORD-002","ORD-003"],
    "AMOUNT":     [1500.0, 200.0, 800.0],
    "STATUS":     ["COMPLETE","PENDING","COMPLETE"],
    "ORDER_DATE": pd.to_datetime(["2024-01-15","2024-01-16","2024-01-17"]),
})

# IMPORTANT: Column names must be UPPERCASE to match Snowflake table (case-insensitive by default)
df.columns = [c.upper() for c in df.columns]

success, nchunks, nrows, _ = write_pandas(
    conn              = conn,
    df                = df,
    table_name        = "STG_ORDERS",    # Must exist in schema
    database          = "ETL_DB",
    schema            = "STAGING",
    auto_create_table = True,            # Creates table if it doesn't exist
    overwrite         = False,           # False = append; True = truncate + load
    chunk_size        = 100_000,         # Rows per chunk
    use_logical_type  = True,            # Preserve date/timestamp types
)

print(f"Success: {success}, Chunks: {nchunks}, Rows: {nrows}")
conn.close()
```

---

## 💻 Example 4: Complete Python ETL Module

```python
import snowflake.connector
import pandas as pd
import os, logging
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger(__name__)

class SnowflakeETL:
    """Reusable ETL class for Snowflake pipelines."""

    def __init__(self):
        self.conn = snowflake.connector.connect(
            account   = os.environ["SF_ACCOUNT"],
            user      = os.environ["SF_USER"],
            password  = os.environ["SF_PASSWORD"],
            warehouse = os.environ.get("SF_WAREHOUSE", "TRANSFORM_WH"),
            database  = os.environ.get("SF_DATABASE",  "ETL_DB"),
        )

    def __enter__(self): return self
    def __exit__(self, *args): self.conn.close()

    def query(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """Execute SELECT and return DataFrame."""
        with self.conn.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(sql, params)
            return pd.DataFrame(cur.fetchall())

    def execute(self, sql: str) -> None:
        """Execute DDL/DML."""
        with self.conn.cursor() as cur:
            cur.execute(sql)
            self.conn.commit()
        logger.info(f"Executed: {sql[:80]}...")

    def load(self, df: pd.DataFrame, table: str, schema: str, overwrite: bool = False) -> int:
        """Load Pandas DataFrame to Snowflake."""
        df.columns = [c.upper() for c in df.columns]
        _, _, nrows, _ = write_pandas(
            self.conn, df, table, schema=schema,
            auto_create_table=True, overwrite=overwrite
        )
        logger.info(f"Loaded {nrows:,} rows to {schema}.{table}")
        return nrows

# Usage:
# with SnowflakeETL() as sf:
#     df = sf.query("SELECT * FROM STAGING.STG_ORDERS WHERE STATUS = %s", ("COMPLETE",))
#     df["REVENUE"] = df["QTY"] * df["UNIT_PRICE"]
#     sf.load(df, "FCT_ORDERS", "ANALYTICS")

print("SnowflakeETL class ready — instantiate with environment variables set")
```

---

## 🏭 Summary

| Method                        | Best For                                   |
| ----------------------------- | ------------------------------------------ |
| `pd.read_sql()` + SQLAlchemy  | Read queries into Pandas                   |
| `write_pandas()`              | Fastest Python → Snowflake load            |
| `executemany()`               | Small bulk inserts (< 1000 rows)           |
| Native connector `DictCursor` | Full control, parameterized queries        |
| `SnowflakeETL` class          | Production pipelines — reusable connection |

---

## ⚠️ Common Mistakes

```python
mistakes = [
    ("Row-by-row INSERT in loop", "Use write_pandas() or executemany() for batch inserts"),
    ("Lowercase column names with write_pandas", "Snowflake columns are uppercase — always .upper() your df columns"),
    ("No connection close", "Always use context manager (with) or call conn.close()"),
    ("String formatting SQL", "NEVER: f\"WHERE x = '{value}'\" — use %s params instead"),
]
for mistake, fix in mistakes:
    print(f"❌ {mistake}")
    print(f"✅ Fix: {fix}\n")
```
