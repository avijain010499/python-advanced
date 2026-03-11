# ⏰ Time Travel & Zero-Copy Cloning in Snowflake

---

## 🤔 What Are These Features?

**Time Travel** — query, restore, or undo changes to any table up to 90 days in the past. Snowflake keeps historical versions of your data automatically.

**Zero-Copy Cloning** — create an instant copy of a database, schema, or table that shares storage with the original (no data duplication, no cost). Changes after cloning write new data — the original is untouched.

---

## 💻 Example 1: Time Travel — Query Past State

```python
time_travel_sql = """
-- 1. Query the table as it was 1 hour ago
SELECT * FROM ETL_DB.ANALYTICS.FCT_ORDERS
AT (OFFSET => -3600);                     -- 3600 seconds = 1 hour ago

-- 2. Query the table at a specific timestamp
SELECT COUNT(*) FROM ETL_DB.ANALYTICS.FCT_ORDERS
AT (TIMESTAMP => '2024-01-15 08:00:00'::TIMESTAMP_NTZ);

-- 3. Query before a specific statement (by query ID)
SELECT * FROM ETL_DB.ANALYTICS.FCT_ORDERS
BEFORE (STATEMENT => '019a8e3d-0504-...');   -- Query ID from History tab

-- 4. Compare current state with state from 24 hours ago
SELECT
    NOW.ORDER_ID,
    HIST.STATUS    AS OLD_STATUS,
    NOW.STATUS     AS NEW_STATUS
FROM ETL_DB.ANALYTICS.FCT_ORDERS AS NOW
JOIN ETL_DB.ANALYTICS.FCT_ORDERS AT (OFFSET => -86400) AS HIST
    ON NOW.ORDER_ID = HIST.ORDER_ID
WHERE NOW.STATUS != HIST.STATUS;           -- Find records whose status changed today
"""
print(time_travel_sql)
```

---

## 💻 Example 2: UNDROP — Recover Dropped Tables

```python
recovery_sql = """
-- Oops! Someone dropped the wrong table
DROP TABLE ETL_DB.ANALYTICS.FCT_ORDERS;   -- Accidental!

-- Recover it (within retention period)
UNDROP TABLE ETL_DB.ANALYTICS.FCT_ORDERS;
-- ✅ Table restored instantly — no data loss!

-- Recover a dropped schema
DROP SCHEMA ETL_DB.ANALYTICS;
UNDROP SCHEMA ETL_DB.ANALYTICS;

-- Recover a dropped database
DROP DATABASE ETL_DB;
UNDROP DATABASE ETL_DB;

-- See dropped objects still in time travel
SHOW TABLES HISTORY IN DATABASE ETL_DB;    -- Lists current AND dropped tables
SHOW SCHEMAS HISTORY IN DATABASE ETL_DB;
"""
print(recovery_sql)
```

---

## 💻 Example 3: Restore a Table to a Previous State

```python
restore_sql = """
-- Scenario: ETL job loaded bad data — need to restore to pre-ETL state

-- Step 1: Find the query ID of the bad load from Query History
-- (Go to Snowsight → Activity → Query History → find the INSERT/LOAD query)

-- Step 2: Create a restored version from before the bad query
CREATE OR REPLACE TABLE ETL_DB.ANALYTICS.FCT_ORDERS_RESTORED
AS
SELECT * FROM ETL_DB.ANALYTICS.FCT_ORDERS
BEFORE (STATEMENT => '019a8e3d-the-bad-query-id');   -- State before the bad query

-- Step 3: Verify the restored data looks correct
SELECT COUNT(*), MIN(ORDER_DATE), MAX(ORDER_DATE)
FROM ETL_DB.ANALYTICS.FCT_ORDERS_RESTORED;

-- Step 4: Swap in the restored table (rename)
ALTER TABLE ETL_DB.ANALYTICS.FCT_ORDERS       RENAME TO FCT_ORDERS_BAD;
ALTER TABLE ETL_DB.ANALYTICS.FCT_ORDERS_RESTORED RENAME TO FCT_ORDERS;

-- Step 5: Clean up
DROP TABLE ETL_DB.ANALYTICS.FCT_ORDERS_BAD;

-- Done! Table restored in minutes, not hours.
"""
print(restore_sql)
```

---

## 💻 Example 4: Zero-Copy Cloning

```python
cloning_sql = """
-- Clone a table (instantaneous! shares storage with original)
CREATE TABLE ETL_DB.STAGING.STG_ORDERS_BACKUP
CLONE ETL_DB.STAGING.STG_ORDERS;
-- Cost: $0 for the clone until you write NEW data to either table

-- Clone an entire schema (for dev environment)
CREATE SCHEMA ETL_DB.STAGING_DEV
CLONE ETL_DB.STAGING;
-- Instant copy of ALL tables in the schema!

-- Clone an entire database (for testing)
CREATE DATABASE ETL_DB_DEV
CLONE ETL_DB;
-- Dev team gets an exact copy of prod — instantly, with minimal cost

-- Use case: create a dev clone of prod before big schema changes
-- Step 1: Clone prod
CREATE DATABASE ETL_DB_FEATURE_BRANCH CLONE ETL_DB;

-- Step 2: Run your new ETL/dbt code against the clone
-- dbt run --target dev --vars "{database: ETL_DB_FEATURE_BRANCH}"

-- Step 3: Verify results in the clone
-- Step 4: Drop clone when done
DROP DATABASE ETL_DB_FEATURE_BRANCH;
"""
print(cloning_sql)
```

---

## 💻 Example 5: Configure Time Travel Retention

```python
retention_config = """
-- Default retention is 1 day for Standard edition, up to 90 for Enterprise
-- Set retention per table:
ALTER TABLE ETL_DB.ANALYTICS.FCT_ORDERS
    SET DATA_RETENTION_TIME_IN_DAYS = 7;   -- 7 days of time travel

-- Set retention at schema level (applies to all tables in schema)
ALTER SCHEMA ETL_DB.ANALYTICS
    SET DATA_RETENTION_TIME_IN_DAYS = 14;

-- Disable time travel for transient tables (reduces storage cost)
CREATE TRANSIENT TABLE ETL_DB.STAGING.STG_ORDERS_TEMP (
    ORDER_ID VARCHAR,
    AMOUNT   NUMBER
);
-- Transient tables: no time travel/failsafe → cheaper for temp ETL tables

-- Failsafe period (Snowflake keeps data 7 additional days after retention expires)
-- You CANNOT query failsafe data yourself — Snowflake support recovers it
"""
print(retention_config)

storage_cost_comparison = """
Storage Cost Comparison:
  Permanent table:  Time Travel (0-90d) + Failsafe (7d) → full cost
  Transient table:  NO Time Travel, NO Failsafe → cheapest (good for staging)
  Temporary table:  Session only, no time travel, auto-dropped on disconnect
"""
print(storage_cost_comparison)
```

---

## 🏭 Summary

| Feature                 | Use Case                                               |
| ----------------------- | ------------------------------------------------------ |
| `AT (OFFSET => -3600)`  | Query table as it was 1 hour ago                       |
| `BEFORE (STATEMENT =>)` | Query state before a specific query ran                |
| `UNDROP TABLE`          | Instantly recover accidentally dropped tables          |
| Zero-copy `CLONE`       | Create dev environment copie of prod — instant & cheap |
| `TRANSIENT TABLE`       | Staging tables — no time travel = lower storage cost   |

---

## ⚠️ Important Notes

```python
notes = [
    "Time travel storage = data * retention_days — can be expensive for large tables, set wisely",
    "UNDROP only works within the retention period (default: 1 day on Standard edition)",
    "Clones share storage — deleting the original does NOT affect the clone",
    "Use TRANSIENT tables for intermediate ETL staging to minimize storage costs",
]
for note in notes:
    print(f"📝 {note}")
```
