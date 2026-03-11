# ❄️ Introduction to Snowflake (ETL Context)

---

## 🤔 What Is Snowflake?

**Snowflake** is a cloud-native data warehouse — designed specifically for analytics and ETL at scale. Unlike traditional on-prem databases, Snowflake:

- Runs entirely on cloud (AWS, Azure, GCP)
- Automatically scales compute up/down
- Separates **storage** from **compute** — pay only for what you use
- Requires zero infrastructure management

> 💡 **Analogy**: A traditional database is like owning a car — you're responsible for maintenance, fuel, parking. Snowflake is like Uber — you request exactly what you need, when you need it, and pay per trip.

---

## 🧱 Snowflake Architecture — 3 Layers

```python
architecture = """
┌──────────────────────────────────────────────────────────┐
│                   CLOUD SERVICES LAYER                   │
│    (Authentication, Query Optimizer, Metadata, Access)   │
├──────────────────────────────────────────────────────────┤
│              COMPUTE LAYER (Virtual Warehouses)           │
│   XS ──── S ──── M ──── L ──── XL ──── 2XL ──── 4XL     │
│   Each warehouse: independent, auto-suspends when idle   │
├──────────────────────────────────────────────────────────┤
│                    STORAGE LAYER                         │
│         (Compressed columnar Parquet on S3/Azure/GCS)    │
│              Shared across ALL warehouses                │
└──────────────────────────────────────────────────────────┘
"""
print(architecture)

key_insight = """
Key insight: Multiple teams can query the SAME data simultaneously
using DIFFERENT warehouses without competing for resources.
- Finance team: their own L warehouse
- Data Science:  their own XL warehouse
- ETL pipelines: their own M warehouse
All reading the same underlying storage — no data duplication!
"""
print(key_insight)
```

---

## 💻 Example 1: Installation & Connection

```python
# Install Snowflake connector
# !pip install snowflake-connector-python
# !pip install snowflake-connector-python[pandas]   # With pandas support
# !pip install snowflake-sqlalchemy                 # For SQLAlchemy integration

import snowflake.connector

conn = snowflake.connector.connect(
    account   = "myorg-myaccount",     # From: Settings → Account → Account Locator
    user      = "ETL_USER",
    password  = "YOUR_PASSWORD",       # Better: use os.environ["SF_PASSWORD"]
    warehouse = "TRANSFORM_WH",
    database  = "ETL_DB",
    schema    = "RAW",
    role      = "TRANSFORMER_ROLE",    # Controls what you can access
)

# Test connection
cursor = conn.cursor()
cursor.execute("SELECT CURRENT_VERSION(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
row = cursor.fetchone()
print(f"Snowflake version: {row[0]}")
print(f"Database: {row[1]}, Schema: {row[2]}")

conn.close()
```

---

## 💻 Example 2: Safer Connection with Environment Variables

```python
import snowflake.connector
import os

def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Create a Snowflake connection from environment variables."""
    return snowflake.connector.connect(
        account   = os.environ["SF_ACCOUNT"],
        user      = os.environ["SF_USER"],
        password  = os.environ["SF_PASSWORD"],
        warehouse = os.environ.get("SF_WAREHOUSE", "COMPUTE_WH"),
        database  = os.environ.get("SF_DATABASE",  "ETL_DB"),
        schema    = os.environ.get("SF_SCHEMA",    "PUBLIC"),
        role      = os.environ.get("SF_ROLE",      "SYSADMIN"),
    )

# Always use context manager — closes connection automatically!
with get_snowflake_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM information_schema.tables")
        print(f"Tables accessible: {cur.fetchone()[0]}")
```

---

## 💻 Example 3: Snowflake Object Hierarchy

```python
hierarchy = """
ORGANIZATION
  └── ACCOUNT  (your company's Snowflake account)
        ├── ROLE  (controls permissions)
        │     ├── ACCOUNTADMIN
        │     ├── SYSADMIN
        │     └── TRANSFORMER_ROLE (custom)
        ├── VIRTUAL WAREHOUSE  (compute)
        │     ├── TRANSFORM_WH  (for ETL)
        │     └── ANALYTICS_WH  (for BI)
        └── DATABASE
              └── SCHEMA
                    ├── TABLE
                    ├── VIEW
                    ├── STAGE  (file staging area)
                    ├── FILE FORMAT
                    ├── STREAM  (CDC)
                    └── TASK   (scheduled SQL)
"""
print(hierarchy)

# SQL to set up context for your session:
setup_sql = """
-- Set session context
USE ROLE TRANSFORMER_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE ETL_DB;
USE SCHEMA RAW_INGESTION;

-- Verify
SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
"""
print("Setup SQL:")
print(setup_sql)
```

---

## 💻 Example 4: Snowflake Pricing — Understanding Credits

```python
credit_info = {
    "1 Credit":               "1 hour of XS warehouse running (price varies by cloud/region)",
    "XS warehouse":           "1 credit/hour  — dev, small queries",
    "S  warehouse":           "2 credits/hour — standard ETL",
    "M  warehouse":           "4 credits/hour — heavy transforms",
    "L  warehouse":           "8 credits/hour — large joins, heavy ELT",
    "Auto-suspend (default)": "Idle warehouse pauses after N minutes → saves credits",
    "Auto-resume":            "Resumes automatically when a query arrives",
}
for key, val in credit_info.items():
    print(f"  {key:<30}: {val}")

cost_tips = """
Cost optimization tips:
1. Use AUTO_SUSPEND = 60 (pause after 1 min idle)
2. Use the SMALLEST warehouse that completes in acceptable time
3. Use separate warehouses for ETL vs BI — different schedules, different sizes
4. Use Query History to find expensive queries
5. Use Clustering on large tables to reduce data scanned
"""
print(cost_tips)
```

---

## 🏭 Summary

| Concept             | What It Is                                    |
| ------------------- | --------------------------------------------- |
| Virtual Warehouse   | Compute cluster — XS to 4XL, pay per second   |
| Database/Schema     | Logical namespace for tables and objects      |
| Stage               | Staging area for files before loading         |
| Auto-suspend/resume | Warehouses pause when idle → cost savings     |
| Role                | Permission level — always use least-privilege |

---

## ⚠️ Common Beginner Mistakes

```python
mistakes = [
    ("Using ACCOUNTADMIN for ETL", "Create a dedicated TRANSFORMER_ROLE with minimal permissions"),
    ("Not setting AUTO_SUSPEND",    "Idle warehouse running 24/7 = wasted credits!"),
    ("Using L warehouse for small queries", "Match warehouse size to workload"),
    ("Storing passwords in code",  "Use environment variables or Prefect blocks"),
]
for mistake, fix in mistakes:
    print(f"❌ {mistake}")
    print(f"✅ Fix: {fix}\n")
```
