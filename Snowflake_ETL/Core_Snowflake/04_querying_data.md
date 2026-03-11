# 🔍 Querying Data in Snowflake (SQL for ETL)

---

## 🤔 What Makes Snowflake SQL Special?

Snowflake SQL is **ANSI-compliant** (standard SQL) but adds powerful extensions for ETL:

- `FLATTEN` for semi-structured / JSON data
- `QUALIFY` for inline window function filtering
- `MERGE` for upserts
- `PIVOT` / `UNPIVOT` for reshaping
- Extensive time/date functions

---

## 💻 Example 1: Core SELECT Patterns for ETL

```python
etl_queries = """
-- 1. Basic transform: clean, cast, filter in one query
SELECT
    UPPER(TRIM(ORDER_ID))                           AS ORDER_ID,
    CAST(CUSTOMER_ID AS NUMBER(18,0))                AS CUSTOMER_ID,
    ROUND(QTY * UNIT_PRICE, 2)                      AS REVENUE,
    CASE
        WHEN STATUS = 'COMPLETE' THEN 'C'
        WHEN STATUS = 'PENDING'  THEN 'P'
        ELSE 'U'
    END                                              AS STATUS_CODE,
    COALESCE(REGION, 'UNKNOWN')                     AS REGION,
    DATEADD('DAY', 30, ORDER_DATE)                  AS DUE_DATE,
    CURRENT_TIMESTAMP()                              AS ETL_LOADED_AT

FROM ETL_DB.RAW_INGESTION.ORDERS_RAW
WHERE ORDER_ID IS NOT NULL
  AND ORDER_DATE >= DATEADD('DAY', -90, CURRENT_DATE());

-- 2. Deduplication using ROW_NUMBER (most common ETL pattern)
WITH DEDUPED AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ORDER_ID
               ORDER BY _EXTRACTED_AT DESC   -- Keep the most recent version
           ) AS RN
    FROM ETL_DB.STAGING.STG_ORDERS
)
SELECT * EXCLUDE (RN)    -- EXCLUDE: Snowflake syntax to drop a column!
FROM DEDUPED
WHERE RN = 1;
"""
print(etl_queries)
```

---

## 💻 Example 2: MERGE — Upsert Pattern (SCD Type 1)

```python
merge_sql = """
-- MERGE: update existing records, insert new ones — atomic operation
MERGE INTO ETL_DB.ANALYTICS.DIM_CUSTOMER AS target
USING (
    SELECT
        CUSTOMER_ID,
        CUSTOMER_NAME,
        EMAIL,
        REGION,
        SEGMENT
    FROM ETL_DB.STAGING.STG_CUSTOMERS
) AS source
ON target.CUSTOMER_ID = source.CUSTOMER_ID  -- Match condition

WHEN MATCHED AND (                           -- Row exists AND data changed
    target.CUSTOMER_NAME != source.CUSTOMER_NAME OR
    target.REGION        != source.REGION
) THEN UPDATE SET
    CUSTOMER_NAME = source.CUSTOMER_NAME,
    REGION        = source.REGION,
    _UPDATED_AT   = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (               -- New record
    CUSTOMER_ID, CUSTOMER_NAME, EMAIL, REGION, SEGMENT, _LOADED_AT
) VALUES (
    source.CUSTOMER_ID, source.CUSTOMER_NAME, source.EMAIL,
    source.REGION, source.SEGMENT, CURRENT_TIMESTAMP()
);
"""
print(merge_sql)
```

---

## 💻 Example 3: Window Functions in Snowflake

```python
window_sql = """
-- Ranking, running totals, lag/lead — same as standard SQL
SELECT
    ORDER_ID,
    CUSTOMER_ID,
    REVENUE,
    ORDER_DATE,
    REGION,

    -- Rank within region
    RANK()        OVER (PARTITION BY REGION ORDER BY REVENUE DESC) AS REGION_RANK,
    DENSE_RANK()  OVER (PARTITION BY REGION ORDER BY REVENUE DESC) AS REGION_DENSE_RANK,

    -- Running total per customer
    SUM(REVENUE)  OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY ORDER_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                               AS RUNNING_TOTAL,

    -- 7-day moving average
    AVG(REVENUE)  OVER (
        PARTITION BY REGION
        ORDER BY ORDER_DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                                               AS MOVING_AVG_7D,

    -- Week-over-week change
    LAG(REVENUE, 7)  OVER (PARTITION BY REGION ORDER BY ORDER_DATE) AS PREV_WEEK_REVENUE,

    -- Snowflake QUALIFY: filter on window result inline (no subquery needed!)
    QUALIFY RANK() OVER (PARTITION BY REGION ORDER BY REVENUE DESC) <= 3
    -- ↑ This keeps only top 3 revenue rows per region — no WHERE needed!

FROM ETL_DB.ANALYTICS.FCT_ORDERS;
"""
print(window_sql)
```

---

## 💻 Example 4: Semi-Structured Data — Query JSON with VARIANT

```python
variant_sql = """
-- Table with a VARIANT column (stores raw JSON)
CREATE TABLE ETL_DB.RAW_INGESTION.EVENTS_RAW (
    EVENT_ID   VARCHAR,
    PAYLOAD    VARIANT,   -- raw JSON blob
    LOADED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert JSON data
INSERT INTO EVENTS_RAW SELECT PARSE_JSON(column1) FROM VALUES
('{"event_id":"E001","user_id":101,"event":"purchase","amount":150.0,"tags":["vip","mobile"]}'),
('{"event_id":"E002","user_id":102,"event":"view","page":"homepage","tags":["desktop"]}');

-- Query JSON fields using : shorthand
SELECT
    PAYLOAD:event_id::VARCHAR             AS EVENT_ID,
    PAYLOAD:user_id::NUMBER               AS USER_ID,
    PAYLOAD:event::VARCHAR                AS EVENT_TYPE,
    PAYLOAD:amount::FLOAT                 AS AMOUNT,
    PAYLOAD:tags[0]::VARCHAR              AS FIRST_TAG,  -- Array index

    -- FLATTEN: explode array into rows (one row per tag)
    F.VALUE::VARCHAR                      AS TAG

FROM EVENTS_RAW,
LATERAL FLATTEN(INPUT => PAYLOAD:tags) F;   -- LATERAL FLATTEN = array explode!
"""
print(variant_sql)
```

---

## 💻 Example 5: Run Queries from Python + Fetch to Pandas

```python
import snowflake.connector, pandas as pd, os

def query_to_df(sql: str, conn_params: dict) -> pd.DataFrame:
    """Execute a Snowflake SQL query and return results as a Pandas DataFrame."""
    with snowflake.connector.connect(**conn_params) as conn:
        with conn.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            df = pd.DataFrame(rows)
    return df

conn_params = {
    "account": os.environ.get("SF_ACCOUNT", "myorg-account"),
    "user":    os.environ.get("SF_USER",    "ETL_USER"),
    "password":os.environ.get("SF_PASSWORD","secret"),
    "warehouse":"TRANSFORM_WH",
    "database": "ETL_DB",
    "schema":   "ANALYTICS",
}

sql = """
    SELECT REGION, SUM(REVENUE) AS TOTAL_REVENUE, COUNT(*) AS ORDERS
    FROM FCT_ORDERS
    WHERE ORDER_DATE >= DATEADD('MONTH', -1, CURRENT_DATE())
    GROUP BY REGION
    ORDER BY TOTAL_REVENUE DESC
"""

# Uncomment when connected:
# df = query_to_df(sql, conn_params)
# print(df.to_string(index=False))

print("Pattern: query_to_df() → Pandas DataFrame → analyze, visualize, or re-load")
```

---

## 🏭 Snowflake SQL ETL Patterns

| Pattern         | SQL Feature                  | Use Case                        |
| --------------- | ---------------------------- | ------------------------------- |
| Deduplication   | `ROW_NUMBER() + QUALIFY`     | Remove duplicate source records |
| Upsert          | `MERGE INTO`                 | SCD Type 1 dimension updates    |
| Incremental     | `WHERE LOADED_AT > last_run` | Process only new rows           |
| JSON parsing    | `PAYLOAD:field::TYPE`        | Extract fields from VARIANT     |
| Array explode   | `LATERAL FLATTEN`            | Normalize nested arrays         |
| Top-N per group | `QUALIFY RANK() <= 3`        | Best/worst performers           |
