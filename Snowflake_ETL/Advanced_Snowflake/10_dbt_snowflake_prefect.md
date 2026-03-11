# 🔗 Snowflake + dbt + Prefect — The Modern Data Stack

---

## 🤔 The Modern Data Stack — How They Fit Together

```python
modern_stack = """
                    THE MODERN DATA STACK

  ┌──────────────┐     ┌─────────────────┐     ┌──────────────────┐
  │  Data Sources │────▶│   Prefect Flow   │────▶│    Snowflake     │
  │  - APIs       │     │   - Orchestrate  │     │  - Raw storage   │
  │  - Databases  │     │   - Schedule     │     │  - Compute (ELT) │
  │  - Files      │     │   - Retry/Alert  │     │  - Streams/Tasks │
  └──────────────┘     └────────┬────────┘     └──────────────────┘
                                │                         ▲
                                │  Triggers               │ SQL Transforms
                                ▼                         │
                       ┌─────────────────┐                │
                       │   dbt Project   │────────────────┘
                       │  - staging/     │
                       │  - marts/       │
                       │  - tests        │
                       └─────────────────┘
                                │
                                ▼
                     ┌─────────────────────┐
                     │   BI Tools / Reports │
                     │  Tableau, Looker, etc│
                     └─────────────────────┘
"""
print(modern_stack)
```

---

## 💻 Example 1: Prefect Flow that Orchestrates the Full ELT

```python
from prefect import flow, task, get_run_logger
from prefect.tasks import exponential_backoff
import snowflake.connector, subprocess, pandas as pd, os, requests
from snowflake.connector.pandas_tools import write_pandas

@task(name="Extract from API", retries=3, retry_delay_seconds=exponential_backoff(2))
def extract_from_api(endpoint: str, date: str) -> pd.DataFrame:
    logger = get_run_logger()
    resp = requests.get(f"{endpoint}?date={date}")
    resp.raise_for_status()
    df = pd.DataFrame(resp.json()["data"])
    logger.info(f"Extracted {len(df):,} rows from {endpoint}")
    return df

@task(name="Load Raw to Snowflake", retries=2)
def load_raw_to_snowflake(df: pd.DataFrame, table: str) -> int:
    logger = get_run_logger()
    df.columns = [c.upper() for c in df.columns]   # Snowflake needs uppercase

    conn = snowflake.connector.connect(
        account   = os.environ["SF_ACCOUNT"],
        user      = os.environ["SF_USER"],
        password  = os.environ["SF_PASSWORD"],
        warehouse = "ETL_WH",
        database  = "ETL_DB",
        schema    = "RAW_INGESTION",
    )
    _, _, nrows, _ = write_pandas(conn, df, table, database="ETL_DB",
                                  schema="RAW_INGESTION", auto_create_table=True)
    conn.close()
    logger.info(f"Loaded {nrows:,} rows to RAW_INGESTION.{table}")
    return nrows

@task(name="Run dbt Build", retries=1)
def run_dbt(command: str, project_dir: str = "/app/dbt") -> None:
    logger = get_run_logger()
    result = subprocess.run(
        f"dbt {command}", shell=True, cwd=project_dir,
        capture_output=True, text=True
    )
    if result.stdout: logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt {command} failed")
    logger.info(f"dbt {command} completed ✅")

@task(name="Validate Data Quality")
def validate_row_counts(date: str) -> None:
    logger = get_run_logger()
    conn = snowflake.connector.connect(
        account=os.environ["SF_ACCOUNT"], user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"], warehouse="ETL_WH",
        database="ETL_DB", schema="ANALYTICS",
    )
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM FCT_ORDERS WHERE ORDER_DATE = %s", (date,))
        count = cur.fetchone()[0]
    conn.close()
    if count == 0:
        raise ValueError(f"No records found for {date} — pipeline may have failed!")
    logger.info(f"DQ passed: {count:,} records loaded for {date}")

@flow(name="Modern Data Stack ELT", log_prints=True)
def full_elt_pipeline(run_date: str = "2024-01-15", dry_run: bool = False):
    logger = get_run_logger()
    logger.info(f"Starting ELT for {run_date}")

    # STEP 1: EXTRACT + LOAD in parallel
    sales_df   = extract_from_api("https://api.company.com/sales", run_date)
    events_df  = extract_from_api("https://api.company.com/events", run_date)

    load_sales  = load_raw_to_snowflake.submit(sales_df, "ORDERS_RAW")
    load_events = load_raw_to_snowflake.submit(events_df, "EVENTS_RAW")
    load_sales.result(); load_events.result()   # Wait for both

    if dry_run:
        logger.info("Dry run — skipping dbt and validation")
        return

    # STEP 2: dbt TRANSFORM (runs SQL inside Snowflake)
    run_dbt("snapshot")                          # SCD dimension history
    run_dbt("build --select staging.* marts.*") # All models + tests

    # STEP 3: VALIDATE
    validate_row_counts(run_date)

    logger.info(f"✅ ELT complete for {run_date}")

# full_elt_pipeline("2024-01-15")
print("Full Modern Data Stack pipeline ready!")
```

---

## 💻 Example 2: dbt Profile for Snowflake (Production Pattern)

```python
dbt_profile = """
# ~/.dbt/profiles.yml

my_project:
  target: prod
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SF_ACCOUNT') }}"
      user:    "{{ env_var('SF_USER') }}"
      password:"{{ env_var('SF_PASSWORD') }}"
      role:    TRANSFORMER_ROLE
      database: ETL_DB
      schema:   DEV_{{ env_var('USER') | upper }}   # e.g., DEV_ALICE (isolated per dev!)
      warehouse: TRANSFORM_WH
      threads:  4

    prod:
      type: snowflake
      account: "{{ env_var('SF_ACCOUNT') }}"
      user:    "{{ env_var('SF_USER') }}"
      password:"{{ env_var('SF_PASSWORD') }}"
      role:    TRANSFORMER_ROLE
      database: ETL_DB
      schema:   ANALYTICS
      warehouse: TRANSFORM_WH_PROD
      threads:  8
"""
print(dbt_profile)
```

---

## 💻 Example 3: Key Integration Patterns Summary

```python
integration_patterns = {
    "Prefect → Snowflake (load)": [
        "Use write_pandas() for DataFrames",
        "Use COPY INTO for large files already in S3",
        "Use Prefect blocks for credentials",
    ],
    "Prefect → dbt (transform)": [
        "subprocess.run('dbt build') from a @task",
        "Or use prefect-dbt package for native integration",
        "Pass --select to run only specific models",
    ],
    "dbt → Snowflake (always)": [
        "dbt profiles.yml defines the Snowflake connection",
        "ref() and source() compile to actual Snowflake table paths",
        "Incremental models use MERGE INTO on Snowflake",
    ],
    "Snowflake → dbt (introspection)": [
        "dbt reads Snowflake INFORMATION_SCHEMA for column types",
        "dbt test SELECTs from Snowflake tables to check data quality",
        "dbt docs generates lineage from Snowflake table metadata",
    ],
}
for pattern, points in integration_patterns.items():
    print(f"\n🔗 {pattern}:")
    for p in points:
        print(f"   • {p}")
```

---

## 🏭 Best Practices Summary

| Layer         | Tool                     | Key Practice                                           |
| ------------- | ------------------------ | ------------------------------------------------------ |
| Orchestration | Prefect                  | `@task` with retries for API/DB calls                  |
| Storage       | Snowflake                | Separate schemas per ELT layer (raw/staging/analytics) |
| Transform     | dbt                      | Use staging→marts layers, always test PKs/FKs          |
| Compute       | Snowflake WH             | `AUTO_SUSPEND=60`, right-size per workload             |
| Credentials   | Prefect Blocks + env_var | Never hardcode credentials in any tool                 |
| CI/CD         | GitHub Actions           | PR → dbt build on dev schema, merge → prod             |
