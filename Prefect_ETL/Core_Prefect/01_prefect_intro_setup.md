# 🌊 Introduction to Prefect (ETL Orchestration)

---

## 🤔 What Is Prefect?

**Prefect** is a Python-native workflow orchestration tool that lets you:

- Define ETL pipelines as **Python functions** decorated with `@flow` and `@task`
- Schedule them to run automatically
- Retry on failure, observe runs, and get alerts

> 💡 **Analogy**: If your ETL code is a recipe, Prefect is the kitchen manager — it ensures the right steps run in the right order, retries if something burns, and tells you when dinner is ready.

**Prefect vs Airflow (the old standard)**:

- Prefect uses pure Python (no DAG syntax, no XML, no operators)
- Easier to test locally — just call `my_flow()`
- Better error messages, modern UI, built-in retry logic

---

## 🧱 Core Concepts

| Concept                  | What It Is                                              |
| ------------------------ | ------------------------------------------------------- |
| **Flow**                 | The top-level pipeline (a Python function with `@flow`) |
| **Task**                 | A single step within a flow (`@task`)                   |
| **Run**                  | One execution of a flow                                 |
| **Deployment**           | A scheduled/triggered version of a flow                 |
| **Work Pool**            | Infrastructure that runs your flows (local, ECS, k8s)   |
| **Prefect Cloud/Server** | UI + scheduling + observability backend                 |

---

## 💻 Example 1: Installation & Setup

```python
# Install Prefect
# !pip install prefect

# Check version
import prefect
print(f"Prefect version: {prefect.__version__}")

# Start the Prefect server (local UI at http://localhost:4200)
# Run in terminal: prefect server start

# Connect to Prefect Cloud (free tier available)
# Run in terminal: prefect cloud login
```

---

## 💻 Example 2: Your First Flow

```python
from prefect import flow, task
import pandas as pd

# Tasks: individual steps — decorated with @task
@task(name="Extract Orders")
def extract_orders(file_path: str) -> pd.DataFrame:
    """Read raw order data from a CSV file."""
    df = pd.read_csv(file_path)
    print(f"Extracted: {len(df):,} rows")
    return df

@task(name="Transform Orders")
def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich order data."""
    df = df.dropna(subset=["order_id"])
    df["revenue"] = df["qty"] * df["unit_price"]
    df["region"]  = df["region"].str.strip().str.upper()
    print(f"Transformed: {len(df):,} rows")
    return df

@task(name="Load Orders")
def load_orders(df: pd.DataFrame, output_path: str) -> None:
    """Save cleaned data to Parquet."""
    df.to_parquet(output_path, index=False)
    print(f"✅ Loaded {len(df):,} rows to {output_path}")

# Flow: the orchestrated pipeline
@flow(name="Daily Sales ETL", log_prints=True)
def daily_sales_etl(input_path: str = "data/orders.csv",
                    output_path: str = "output/orders_clean.parquet"):
    """Complete ETL pipeline for daily sales data."""
    df_raw   = extract_orders(input_path)
    df_clean = transform_orders(df_raw)
    load_orders(df_clean, output_path)
    print("✅ Pipeline complete!")

# Run it — just call the function!
if __name__ == "__main__":
    daily_sales_etl()
```

---

## 💻 Example 3: What Prefect Adds Automatically

```python
# When you run a @flow, Prefect automatically:
# 1. Creates a run record with a unique ID and timestamp
# 2. Tracks the state of each @task (Pending → Running → Completed/Failed)
# 3. Captures all print() output as logs (with log_prints=True)
# 4. Handles retries if you configure them
# 5. Reports completion/failure to the UI

# Run and see: Prefect UI at http://localhost:4200
# (or Prefect Cloud if using cloud)

from prefect import flow

@flow(name="Hello Prefect")
def hello():
    print("This is tracked by Prefect!")
    return 42

result = hello()
print(f"Flow returned: {result}")
# Prefect UI will show:
# Flow Run: hello/elegant-fox (auto-generated name)
# State: Completed
# Duration: 0.12s
```

---

## 🏭 Summary

| Concept    | dbt World Equivalent | What It Does                         |
| ---------- | -------------------- | ------------------------------------ |
| `@flow`    | `dbt run`            | Orchestrates all tasks in order      |
| `@task`    | A single dbt model   | One unit of work                     |
| Flow run   | dbt job execution    | One instance of running the pipeline |
| Deployment | dbt job schedule     | Scheduled/triggered flow             |
| Prefect UI | dbt Cloud UI         | Observe, monitor, trigger runs       |

---

## ⚠️ Common Beginners' Mistakes

```python
mistakes = [
    ("Not calling flow as a function for testing", "Just call daily_sales_etl() locally!"),
    ("Putting all logic in the @flow", "Logic belongs in @task functions — flow just calls tasks"),
    ("No return types on tasks", "Always return data from tasks — Prefect tracks it"),
]
for mistake, fix in mistakes:
    print(f"❌ {mistake}")
    print(f"✅ Fix: {fix}\n")
```
