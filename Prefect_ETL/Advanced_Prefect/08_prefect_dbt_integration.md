# 🤝 Prefect + dbt Integration

---

## 🤔 Why Combine Prefect and dbt?

- **Prefect** handles orchestration: scheduling, retries, alerts, API calls, file moves
- **dbt** handles SQL transformation inside the warehouse

Combining them gives you a **full modern data pipeline**: Prefect extracts and loads raw data, then triggers dbt to transform it, then Prefect runs tests and sends notifications.

---

## 💻 Example 1: Run dbt from Prefect Using Shell

```python
from prefect import flow, task
import subprocess

@task(name="Run dbt models", retries=1)
def run_dbt(command: str, working_dir: str = "/app") -> dict:
    """Execute a dbt CLI command and return results."""
    result = subprocess.run(
        f"dbt {command}",
        shell=True,
        cwd=working_dir,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"STDERR: {result.stderr}")
        raise RuntimeError(f"dbt {command} failed with exit code {result.returncode}")
    return {"command": command, "returncode": result.returncode}

@flow(name="Prefect + dbt ETL", log_prints=True)
def prefect_dbt_pipeline(run_date: str = "2024-01-15"):
    # Step 1: Check dbt connection
    run_dbt("debug")

    # Step 2: Run dbt snapshots (SCD dimension tracking)
    run_dbt("snapshot")

    # Step 3: Run all models
    run_dbt("run --select staging.* intermediate.* marts.*")

    # Step 4: Run tests
    run_dbt("test")

    # Step 5: Refresh docs
    run_dbt("docs generate")

    print(f"✅ Full dbt pipeline complete for {run_date}")

# prefect_dbt_pipeline()
```

---

## 💻 Example 2: prefect-dbt Integration (Official Package)

```python
# pip install prefect-dbt

from prefect_dbt.cli.commands import DbtCoreOperation
from prefect import flow, task

@task(name="dbt Run")
def dbt_run(select: str = None) -> None:
    """Run dbt models using the official Prefect-dbt integration."""
    cmd = ["run"]
    if select:
        cmd += ["--select", select]

    DbtCoreOperation(
        commands          = [" ".join(["dbt"] + cmd)],
        project_dir       = "/app/dbt_project",
        profiles_dir      = "/app/dbt_project",
        dbt_cli_profile   = "snowflake-profile",
    ).run()

@task(name="dbt Test")
def dbt_test(select: str = None) -> None:
    cmd = ["dbt", "test"]
    if select:
        cmd += ["--select", select]

    DbtCoreOperation(
        commands    = [" ".join(cmd)],
        project_dir = "/app/dbt_project",
    ).run()

@flow(name="Prefect dbt Orchestration", log_prints=True)
def orchestrated_pipeline(select_models: str = None):
    dbt_run(select_models)
    dbt_test(select_models)
    print("dbt pipeline complete!")

# orchestrated_pipeline("staging.*")
```

---

## 💻 Example 3: Full ELT Pipeline (Extract → Load → dbt Transform)

```python
from prefect import flow, task
import pandas as pd
import subprocess

@task(name="Extract from API", retries=3, retry_delay_seconds=30)
def extract_from_api(endpoint: str, date: str) -> pd.DataFrame:
    """Extract raw data from REST API."""
    import requests
    resp = requests.get(f"{endpoint}?date={date}")
    resp.raise_for_status()
    df = pd.DataFrame(resp.json()["records"])
    print(f"Extracted {len(df):,} rows from {endpoint}")
    return df

@task(name="Load Raw to Warehouse")
def load_raw(df: pd.DataFrame, table: str) -> int:
    """Load raw (unprocessed) data to staging schema in warehouse."""
    from sqlalchemy import create_engine
    import os
    engine = create_engine(os.environ["WAREHOUSE_URL"])
    df.to_sql(table, engine, schema="raw_ingestion", if_exists="append", index=False)
    print(f"Loaded {len(df):,} rows to raw_ingestion.{table}")
    return len(df)

@task(name="Run dbt Transformation", retries=1)
def dbt_transform(select: str) -> None:
    result = subprocess.run(
        f"dbt build --select {select}",
        shell=True, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt failed:\n{result.stderr}")
    print(result.stdout)

@flow(name="Complete ELT Pipeline", log_prints=True)
def complete_elt(run_date: str = "2024-01-15"):
    """
    Full ELT: Extract from API → Load raw → dbt transforms
    """
    # EXTRACT
    sales_df  = extract_from_api("https://api.company.com/sales", run_date)
    events_df = extract_from_api("https://api.company.com/events", run_date)

    # LOAD (parallel)
    s_future = load_raw.submit(sales_df, "orders")
    e_future = load_raw.submit(events_df, "user_events")
    s_future.result(); e_future.result()  # Wait for both

    # TRANSFORM (dbt runs SQL in the warehouse)
    dbt_transform("staging.stg_orders+ staging.stg_events+")

    print(f"✅ Complete ELT pipeline done for {run_date}")

# complete_elt("2024-01-15")
```

---

## 🏭 Integration Architecture

```python
architecture = """
┌─────────────────────────────────────────────────────┐
│                  PREFECT FLOW                        │
│                                                     │
│  @task                  @task                ↗ Slack Alert
│  Extract from API  →    Load raw to S3/DW   │       │
│                         (pandas/SQLAlchemy) │   ERROR│
│                                ↓            │       │
│                         @task               │       │
│                         dbt build ──────────┘       │
│                         (staging + marts)           │
│                                ↓                    │
│                         @task                       │
│                         dbt test                    │
│                                ↓                    │
│                         @task                       │
│                         Send success notification   │
└─────────────────────────────────────────────────────┘
"""
print(architecture)
```
