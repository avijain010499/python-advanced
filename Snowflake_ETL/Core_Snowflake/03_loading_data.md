# 📥 Loading Data into Snowflake (ETL Extract → Load)

---

## 🤔 How Does Data Get Into Snowflake?

Snowflake uses a **staging** system — files land in a **Stage** (S3, Azure Blob, GCS, or Snowflake internal storage), then a `COPY INTO` command bulk-loads them at very high speed.

> 💡 **Never INSERT row by row** into Snowflake — always COPY from a file/stage. Even from Python, generate a file and COPY it — it's orders of magnitude faster.

---

## 🧱 Loading Methods

| Method                   | Best For                               | Speed      |
| ------------------------ | -------------------------------------- | ---------- |
| `COPY INTO` from stage   | Main ETL loading (CSV, JSON, Parquet)  | ⭐⭐⭐⭐⭐ |
| `PUT` + `COPY INTO`      | Load files from local disk via Python  | ⭐⭐⭐⭐   |
| `write_pandas()`         | Load Pandas DF via Arrow (Python only) | ⭐⭐⭐     |
| `INSERT INTO ... VALUES` | Small lookups only (<100 rows)         | ⭐         |

---

## 💻 Example 1: Create a Stage (Staging Area for Files)

```python
stage_sql = """
-- Internal stage (Snowflake stores files for you)
CREATE STAGE IF NOT EXISTS ETL_DB.RAW_INGESTION.SALES_STAGE
    COMMENT = 'Internal stage for incoming sales CSV files';

-- External stage (files already in S3)
CREATE STAGE IF NOT EXISTS ETL_DB.RAW_INGESTION.S3_SALES_STAGE
    URL = 's3://my-etl-bucket/raw/sales/'
    CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...')
    COMMENT = 'S3 external stage — points to raw sales files';

-- List files in stage
LIST @ETL_DB.RAW_INGESTION.SALES_STAGE;
"""
print(stage_sql)
```

---

## 💻 Example 2: Create a File Format

```python
file_formats_sql = """
-- CSV file format (most common)
CREATE OR REPLACE FILE FORMAT ETL_DB.RAW_INGESTION.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\\n'
    SKIP_HEADER = 1               -- Skip the header row
    NULL_IF = ('NULL', 'null', '', 'N/A', '\\\\N')  -- Treat these as NULL
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE             -- Trim whitespace from values
    DATE_FORMAT = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;  -- Allow extra/missing columns

-- JSON file format
CREATE OR REPLACE FILE FORMAT ETL_DB.RAW_INGESTION.JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE      -- Remove outer [] from JSON arrays
    COMPRESSION = 'AUTO';

-- Parquet file format
CREATE OR REPLACE FILE FORMAT ETL_DB.RAW_INGESTION.PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY';
"""
print(file_formats_sql)
```

---

## 💻 Example 3: COPY INTO — Bulk Load from Stage

```python
copy_into_sql = """
-- Load all CSV files from stage into STG_ORDERS table
COPY INTO ETL_DB.STAGING.STG_ORDERS (
    ORDER_ID, CUSTOMER_ID, PRODUCT_ID, QTY, UNIT_PRICE,
    STATUS, REGION, ORDER_DATE, _SOURCE_FILE
)
FROM (
    SELECT
        $1::VARCHAR     AS ORDER_ID,
        $2::NUMBER      AS CUSTOMER_ID,
        $3::NUMBER      AS PRODUCT_ID,
        $4::NUMBER      AS QTY,
        $5::NUMBER(18,4) AS UNIT_PRICE,
        UPPER(TRIM($6)) AS STATUS,
        UPPER(TRIM($7)) AS REGION,
        $8::DATE        AS ORDER_DATE,
        METADATA$FILENAME AS _SOURCE_FILE    -- Which file this row came from
    FROM @ETL_DB.RAW_INGESTION.SALES_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'ETL_DB.RAW_INGESTION.CSV_FORMAT')
ON_ERROR = 'CONTINUE'      -- Skip bad rows, don't fail the whole load
PURGE = FALSE              -- Keep files in stage after loading (for reprocessing)
;

-- Check results
SELECT *
FROM TABLE(VALIDATE(ETL_DB.STAGING.STG_ORDERS, JOB_ID => '_last'))
LIMIT 10;   -- Shows rows that were rejected
"""
print(copy_into_sql)
```

---

## 💻 Example 4: Load from Pandas DataFrame (Python)

```python
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os

# Create a sample Pandas DataFrame
df = pd.DataFrame({
    "ORDER_ID":    ["ORD-001", "ORD-002", "ORD-003"],
    "CUSTOMER_ID": [101, 102, 103],
    "AMOUNT":      [1500.0, 200.0, 800.0],
    "STATUS":      ["COMPLETE", "PENDING", "COMPLETE"],
    "ORDER_DATE":  ["2024-01-15", "2024-01-16", "2024-01-17"],
})

# Connect and load
conn = snowflake.connector.connect(
    account   = os.environ.get("SF_ACCOUNT",  "myorg-account"),
    user      = os.environ.get("SF_USER",     "ETL_USER"),
    password  = os.environ.get("SF_PASSWORD", "secret"),
    warehouse = "TRANSFORM_WH",
    database  = "ETL_DB",
    schema    = "STAGING",
)

# write_pandas uses Arrow internally (very fast!)
success, nchunks, nrows, _ = write_pandas(
    conn       = conn,
    df         = df,
    table_name = "STG_ORDERS",
    database   = "ETL_DB",
    schema     = "STAGING",
    auto_create_table = True,    # Create table if it doesn't exist
    overwrite         = False,   # Append (not replace)
)
print(f"✅ Loaded: success={success}, chunks={nchunks}, rows={nrows}")
conn.close()
```

---

## 💻 Example 5: Full Python ETL — Extract, Write CSV, PUT, COPY

```python
import pandas as pd
import snowflake.connector, os, io

def load_dataframe_to_snowflake(
    df: pd.DataFrame,
    target_table: str,
    conn_params: dict,
    mode: str = "append",  # "append" or "overwrite"
) -> int:
    """Best-practice pattern: write Parquet, PUT to internal stage, COPY INTO."""
    with snowflake.connector.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            stage = f"@~/{target_table}_staging"

            # Write DataFrame to Parquet in memory
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            # PUT to Snowflake internal user stage
            cur.execute(f"PUT file:///dev/stdin @~/{target_table}.parquet OVERWRITE=TRUE",
                        file_stream=buffer)

            # COPY INTO target table
            if mode == "overwrite":
                cur.execute(f"TRUNCATE TABLE IF EXISTS {target_table}")

            cur.execute(f"""
                COPY INTO {target_table}
                FROM @~/{target_table}.parquet
                FILE_FORMAT = (TYPE='PARQUET')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = 'ABORT_STATEMENT'
            """)
            result = cur.fetchall()
            rows_loaded = sum(r[3] for r in result)
            print(f"✅ {rows_loaded:,} rows loaded to {target_table}")
            return rows_loaded

# Usage:
# rows = load_dataframe_to_snowflake(df, "ETL_DB.STAGING.STG_ORDERS", conn_params)
print("Pattern: Pandas → Parquet → PUT → COPY INTO (fastest approach)")
```

---

## 🏭 Loading Strategy Summary

| Scenario              | Recommended Approach                             |
| --------------------- | ------------------------------------------------ |
| Files in S3/Azure/GCS | External stage + `COPY INTO`                     |
| Files on local disk   | `PUT` to internal stage, then `COPY INTO`        |
| Pandas DataFrame      | `write_pandas()` (< 100MB) or PUT/COPY (> 100MB) |
| Small lookup data     | `write_pandas()` with `auto_create_table=True`   |
| Real-time inserts     | Use Kafka → Snowpipe (not covered here)          |
