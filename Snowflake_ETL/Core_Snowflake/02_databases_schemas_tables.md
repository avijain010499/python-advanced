# 🗄️ Databases, Schemas & Tables in Snowflake

---

## 🤔 Why Does Structure Matter?

In Snowflake, how you organize databases, schemas, and tables affects:

- **Access control** (who can see what)
- **Cost** (schema-level warehouse assignment, cloning)
- **Maintainability** (findability, naming conventions)

---

## 💻 Example 1: Creating the ETL Object Hierarchy

```python
ddl_structure = """
-- Standard ETL database structure in Snowflake

-- Create the main ETL database
CREATE DATABASE IF NOT EXISTS ETL_DB;
USE DATABASE ETL_DB;

-- Schema layer: separate by data maturity (ELT pattern)
CREATE SCHEMA IF NOT EXISTS RAW_INGESTION;     -- Raw source data, no transforms
CREATE SCHEMA IF NOT EXISTS STAGING;           -- Cleaned, typed, deduplicated
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;      -- Business logic joins
CREATE SCHEMA IF NOT EXISTS ANALYTICS;         -- Final marts for BI tools
CREATE SCHEMA IF NOT EXISTS REFERENCE_DATA;    -- Lookup tables, mapping tables
CREATE SCHEMA IF NOT EXISTS AUDIT;             -- ETL run logs, row counts

-- Grant access
GRANT USAGE ON DATABASE ETL_DB TO ROLE TRANSFORMER_ROLE;
GRANT ALL ON SCHEMA RAW_INGESTION TO ROLE TRANSFORMER_ROLE;
"""
print(ddl_structure)
```

---

## 💻 Example 2: Snowflake Data Types Reference

```python
import snowflake.connector, os

data_types = {
    "STRING / VARCHAR(n)": "Text — VARCHAR(255), VARCHAR(16777216) for large text",
    "NUMBER(p, s)":        "Exact numeric — NUMBER(18,2) for money",
    "FLOAT / DOUBLE":      "Approximate numeric — for analytics",
    "BOOLEAN":             "TRUE / FALSE / NULL",
    "DATE":                "Date only: 2024-01-15",
    "TIMESTAMP_NTZ":       "Datetime, NO timezone (default for ETL)",
    "TIMESTAMP_TZ":        "Datetime WITH timezone (user events, logs)",
    "VARIANT":             "Semi-structured: JSON, Avro, Parquet (any schema)",
    "ARRAY":               "Array of values (inside VARIANT)",
    "OBJECT":              "JSON object (inside VARIANT)",
}
print("Snowflake Data Types:")
for dtype, desc in data_types.items():
    print(f"  {dtype:<25}: {desc}")
```

---

## 💻 Example 3: Creating ETL Tables with Best-Practice DDL

```python
create_tables_sql = """
USE SCHEMA ETL_DB.STAGING;

-- Staging table: sales orders (one per source system record)
CREATE OR REPLACE TABLE STG_ORDERS (
    ORDER_ID         VARCHAR(50)    NOT NULL,     -- Natural key from source
    CUSTOMER_ID      NUMBER(18,0)   NOT NULL,
    PRODUCT_ID       NUMBER(18,0),
    QTY              NUMBER(10,0),
    UNIT_PRICE       NUMBER(18,4),                -- 4 decimals for precision
    STATUS           VARCHAR(20),
    REGION           VARCHAR(50),
    ORDER_DATE       DATE,
    CREATED_AT       TIMESTAMP_NTZ,
    -- ETL audit columns (every staging table should have these)
    _EXTRACTED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE     VARCHAR(500),                -- Which file this row came from
    _ROW_NUMBER      NUMBER(18,0),               -- Row number in source file
    -- DDL options
    CONSTRAINT PK_STG_ORDERS PRIMARY KEY (ORDER_ID)
)
CLUSTER BY (ORDER_DATE, REGION)   -- Pruning optimization for common filters
COMMENT = 'Cleaned orders from ERP system. Loaded daily at 5am UTC.';

-- Dimension table
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    CUSTOMER_SK      NUMBER AUTOINCREMENT PRIMARY KEY,  -- Surrogate key
    CUSTOMER_ID      NUMBER NOT NULL,                   -- Natural key
    CUSTOMER_NAME    VARCHAR(200),
    EMAIL            VARCHAR(500),
    REGION           VARCHAR(50),
    SEGMENT          VARCHAR(50),
    -- SCD Type 2 columns
    VALID_FROM       DATE    NOT NULL DEFAULT CURRENT_DATE(),
    VALID_TO         DATE,     -- NULL = current record
    IS_CURRENT       BOOLEAN DEFAULT TRUE,
    _LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""
print(create_tables_sql)
```

---

## 💻 Example 4: Execute DDL via Python

```python
import snowflake.connector, os

def execute_ddl(ddl: str, conn_params: dict) -> None:
    """Execute DDL statement(s) in Snowflake."""
    with snowflake.connector.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            for statement in ddl.strip().split(";"):
                statement = statement.strip()
                if statement:
                    cur.execute(statement)
                    print(f"✅ Executed: {statement[:60]}...")

conn_params = {
    "account":   os.environ.get("SF_ACCOUNT",   "myorg-myaccount"),
    "user":      os.environ.get("SF_USER",       "ETL_USER"),
    "password":  os.environ.get("SF_PASSWORD",   "secret"),
    "warehouse": "TRANSFORM_WH",
    "database":  "ETL_DB",
    "schema":    "STAGING",
}

ddl = """
CREATE TABLE IF NOT EXISTS TEST_TABLE (ID NUMBER, NAME VARCHAR(100));
INSERT INTO TEST_TABLE VALUES (1, 'Alice'), (2, 'Bob')
"""

# Uncomment to run:
# execute_ddl(ddl, conn_params)
print("DDL ready — connect and execute when Snowflake credentials are available")
```

---

## 💻 Example 5: Information Schema — Discover Table Metadata

```python
discovery_queries = {
    "List all tables in a schema": """
        SELECT TABLE_NAME, ROW_COUNT, BYTES, LAST_ALTERED
        FROM ETL_DB.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'STAGING'
        ORDER BY LAST_ALTERED DESC;
    """,
    "Get column details": """
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
        FROM ETL_DB.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'STG_ORDERS'
        ORDER BY ORDINAL_POSITION;
    """,
    "Find the most recently modified tables": """
        SELECT TABLE_SCHEMA, TABLE_NAME,
               ROW_COUNT, BYTES / 1024 / 1024 AS SIZE_MB,
               LAST_ALTERED
        FROM ETL_DB.INFORMATION_SCHEMA.TABLES
        ORDER BY LAST_ALTERED DESC LIMIT 10;
    """,
}
for name, query in discovery_queries.items():
    print(f"-- {name}")
    print(query)
```

---

## 🏭 Summary

| Object                 | Purpose                                | ETL Layer          |
| ---------------------- | -------------------------------------- | ------------------ |
| `RAW_INGESTION` schema | Store untouched source files           | Extract → Load     |
| `STAGING` schema       | Cleaned, typed data                    | dbt staging models |
| `ANALYTICS` schema     | Final marts for BI                     | dbt marts          |
| `CLUSTER BY`           | Speed up large table scans             | Fact tables        |
| `AUTOINCREMENT`        | Auto-generate surrogate keys           | Dimension tables   |
| `_EXTRACTED_AT` column | Audit trail — when was this row loaded | All tables         |
