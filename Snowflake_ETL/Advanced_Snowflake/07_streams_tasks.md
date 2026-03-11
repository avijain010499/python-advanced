# 🔄 Streams & Tasks — CDC and Automation in Snowflake

---

## 🤔 What Are Streams and Tasks?

**Streams** = Change Data Capture (CDC) — track INSERTs, UPDATEs, DELETEs on a table since the last time you consumed the stream. Like a queue of changes.

**Tasks** = Scheduled SQL or Stored Procedure execution — Snowflake's built-in job scheduler.

Together they enable **automated, incremental ETL pipelines entirely inside Snowflake** — no external orchestrator needed for simple workflows.

---

## 💻 Example 1: Create a Stream on a Table

```python
stream_sql = """
-- Create a stream that captures ALL changes to STG_ORDERS
CREATE OR REPLACE STREAM ETL_DB.STAGING.STG_ORDERS_STREAM
    ON TABLE ETL_DB.STAGING.STG_ORDERS
    APPEND_ONLY = FALSE   -- Capture inserts, updates, AND deletes
    -- APPEND_ONLY = TRUE  -- Only track new rows (faster, lower storage)
    COMMENT = 'CDC stream on STG_ORDERS — captures changes for incremental ETL';

-- An APPEND_ONLY stream is more efficient — use when you only INSERT new rows
CREATE OR REPLACE STREAM ETL_DB.STAGING.STG_EVENTS_STREAM
    ON TABLE ETL_DB.RAW_INGESTION.EVENTS_RAW
    APPEND_ONLY = TRUE;    -- Events table: only inserts, never updates/deletes
"""
print(stream_sql)
```

---

## 💻 Example 2: Read from a Stream

```python
query_stream_sql = """
-- Query the stream to see pending changes
SELECT *
FROM ETL_DB.STAGING.STG_ORDERS_STREAM
LIMIT 10;

-- Stream metadata columns (added automatically by Snowflake):
-- METADATA$ACTION:    'INSERT' or 'DELETE'
-- METADATA$ISUPDATE:  TRUE if this row is part of an UPDATE operation
-- METADATA$ROW_ID:    Internal row identifier

-- For an UPDATE, Snowflake creates TWO rows:
--   1. DELETE (the old version)
--   2. INSERT (the new version)
-- Filter to just the new value:
SELECT *
FROM ETL_DB.STAGING.STG_ORDERS_STREAM
WHERE METADATA$ACTION = 'INSERT';   -- Only the new/updated version

-- Check if stream has data (use in Task condition)
SELECT SYSTEM$STREAM_HAS_DATA('ETL_DB.STAGING.STG_ORDERS_STREAM');
-- Returns 'true' or 'false'
"""
print(query_stream_sql)
```

---

## 💻 Example 3: MERGE Using Stream — Incremental ETL Pattern

```python
merge_from_stream_sql = """
-- This runs the incremental logic: apply only changed rows to the target
MERGE INTO ETL_DB.ANALYTICS.FCT_ORDERS AS target
USING (
    -- Get only new/updated records from the stream
    SELECT
        ORDER_ID, CUSTOMER_ID, AMOUNT, STATUS, REGION, ORDER_DATE,
        METADATA$ACTION    AS ACTION,
        METADATA$ISUPDATE  AS IS_UPDATE
    FROM ETL_DB.STAGING.STG_ORDERS_STREAM
    WHERE METADATA$ACTION = 'INSERT'           -- Inserts and new side of updates
) AS source
ON target.ORDER_ID = source.ORDER_ID

WHEN MATCHED THEN UPDATE SET                   -- Record already in target → update it
    AMOUNT     = source.AMOUNT,
    STATUS     = source.STATUS,
    REGION     = source.REGION,
    _UPDATED_AT = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (                 -- New record → insert
    ORDER_ID, CUSTOMER_ID, AMOUNT, STATUS, REGION, ORDER_DATE, _LOADED_AT
) VALUES (
    source.ORDER_ID, source.CUSTOMER_ID, source.AMOUNT,
    source.STATUS, source.REGION, source.ORDER_DATE, CURRENT_TIMESTAMP()
);
-- After a successful MERGE, the stream offset advances — consumed records disappear!
"""
print(merge_from_stream_sql)
```

---

## 💻 Example 4: Tasks — Snowflake's Built-In Scheduler

```python
task_sql = """
-- Create a task to run MERGE from stream every 5 minutes
CREATE OR REPLACE TASK ETL_DB.STAGING.ORDERS_INCREMENTAL_TASK
    WAREHOUSE   = TRANSFORM_WH
    SCHEDULE    = '5 MINUTE'                      -- Cron or interval
    WHEN SYSTEM$STREAM_HAS_DATA('ETL_DB.STAGING.STG_ORDERS_STREAM')  -- Only if data!
    AS
    -- The SQL to run (the MERGE statement from above)
    MERGE INTO ETL_DB.ANALYTICS.FCT_ORDERS AS target
    USING (
        SELECT *, METADATA$ACTION AS ACTION
        FROM ETL_DB.STAGING.STG_ORDERS_STREAM
        WHERE METADATA$ACTION = 'INSERT'
    ) AS source
    ON target.ORDER_ID = source.ORDER_ID
    WHEN MATCHED THEN UPDATE SET AMOUNT = source.AMOUNT, STATUS = source.STATUS
    WHEN NOT MATCHED THEN INSERT (ORDER_ID, CUSTOMER_ID, AMOUNT, STATUS, ORDER_DATE)
        VALUES (source.ORDER_ID, source.CUSTOMER_ID, source.AMOUNT, source.STATUS, source.ORDER_DATE);

-- Tasks are SUSPENDED by default — enable it!
ALTER TASK ETL_DB.STAGING.ORDERS_INCREMENTAL_TASK RESUME;

-- Operations:
ALTER TASK ORDERS_INCREMENTAL_TASK SUSPEND;     -- Pause
ALTER TASK ORDERS_INCREMENTAL_TASK RESUME;      -- Enable
EXECUTE TASK ORDERS_INCREMENTAL_TASK;           -- Run immediately (manual trigger)

-- View task history
SELECT *
FROM TABLE(ETL_DB.INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'ORDERS_INCREMENTAL_TASK'))
ORDER BY SCHEDULED_TIME DESC LIMIT 10;
"""
print(task_sql)
```

---

## 💻 Example 5: Task DAG — Chain Tasks Together

```python
task_dag_sql = """
-- Root task: runs every day at 5am UTC
CREATE OR REPLACE TASK ROOT_ETL_TASK
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE  = 'USING CRON 0 5 * * * UTC'   -- 5am UTC daily
    AS CALL ETL_DB.STAGING.SP_EXTRACT_NEW_DATA();  -- Call stored procedure

-- Child task: runs AFTER root task completes
CREATE OR REPLACE TASK TRANSFORM_TASK
    WAREHOUSE = TRANSFORM_WH
    AFTER ETL_DB.STAGING.ROOT_ETL_TASK       -- Dependency!
    AS
    INSERT INTO ETL_DB.ANALYTICS.FCT_ORDERS
    SELECT * FROM ETL_DB.STAGING.STG_ORDERS WHERE STATUS = 'COMPLETE';

-- Second child task: runs AFTER root task completes (parallel with TRANSFORM_TASK)
CREATE OR REPLACE TASK QC_TASK
    WAREHOUSE = TRANSFORM_WH
    AFTER ETL_DB.STAGING.ROOT_ETL_TASK
    AS
    INSERT INTO ETL_DB.AUDIT.QC_LOG
    SELECT CURRENT_TIMESTAMP(), COUNT(*) FROM ETL_DB.STAGING.STG_ORDERS;

-- Enable all tasks (must start from root)
ALTER TASK ROOT_ETL_TASK RESUME;
ALTER TASK TRANSFORM_TASK RESUME;
ALTER TASK QC_TASK RESUME;
"""
print(task_dag_sql)
```

---

## 🏭 Streams + Tasks Summary

| Feature                    | Use Case                                    |
| -------------------------- | ------------------------------------------- |
| Stream (append-only)       | Track new events — clickstream, logs, IoT   |
| Stream (full)              | Track updates/deletes — dimension changes   |
| `SYSTEM$STREAM_HAS_DATA()` | Avoid running tasks when no new data        |
| Task + MERGE from stream   | Incremental ETL — process only changed rows |
| Task DAG (`AFTER`)         | Chain steps: extract → transform → QC       |
