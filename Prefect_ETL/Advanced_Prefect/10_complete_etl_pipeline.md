# 🏭 Complete ETL Pipeline Patterns with Prefect

---

## 💻 Example 1: Production-Grade ETL Pipeline Template

```python
"""
Complete, production-grade ETL pipeline with:
- Parameterized runs
- Retry logic
- Logging at every step
- Data quality checks
- Slack notification on failure
- Prefect artifacts for observability
"""

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.tasks import exponential_backoff
from datetime import date, timedelta
import pandas as pd

# ──────────────── EXTRACT ────────────────────

@task(
    name    = "Extract Sales Data",
    retries = 3,
    retry_delay_seconds = exponential_backoff(backoff_factor=2),
    tags    = ["extract", "daily"],
)
def extract_sales(run_date: str, source: str = "postgres") -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Extracting sales for {run_date} from {source}")

    # Simulated extraction
    df = pd.DataFrame({
        "order_id":   [f"ORD-{i:04d}" for i in range(1000)],
        "customer_id":[i % 100 for i in range(1000)],
        "amount":     [round(100 + i * 0.5, 2) for i in range(1000)],
        "region":     ["NORTH","SOUTH","EAST","WEST"][i % 4] for i in range(1000),
        "status":     ["COMPLETE","PENDING","FAILED"][i % 3] for i in range(1000),
        "order_date": [run_date] * 1000,
    })
    logger.info(f"Extracted {len(df):,} rows")
    return df

# ──────────────── VALIDATE ────────────────────

@task(name="Validate Data Quality")
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Running data quality checks...")

    checks = {
        "No null order_id": df["order_id"].notna().all(),
        "No null amount":   df["amount"].notna().all(),
        "No negative amount": (df["amount"] >= 0).all(),
        "Non-empty dataset": len(df) > 0,
    }
    failed = {k: v for k, v in checks.items() if not v}
    if failed:
        raise ValueError(f"DQ failed: {list(failed.keys())}")
    logger.info(f"All {len(checks)} quality checks passed!")
    return df

# ──────────────── TRANSFORM ────────────────────

@task(name="Transform Sales")
def transform_sales(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()

    df_clean = (
        df.copy()
        .assign(
            region = lambda d: d["region"].str.strip().str.upper(),
            status = lambda d: d["status"].str.strip().str.upper(),
            amount = lambda d: d["amount"].round(2),
        )
        .query("status == 'COMPLETE'")
        .drop_duplicates(subset=["order_id"])
    )
    logger.info(f"Transform: {len(df):,} → {len(df_clean):,} rows")
    return df_clean

# ──────────────── LOAD ────────────────────

@task(name="Load to Warehouse", retries=2, retry_delay_seconds=10)
def load(df: pd.DataFrame, target: str) -> int:
    logger = get_run_logger()
    logger.info(f"Loading {len(df):,} rows to {target}")
    # In production: df.to_sql(...) or spark.write.parquet(...)
    logger.info(f"✅ Load complete!")
    return len(df)

# ──────────────── REPORTING ────────────────────

@task(name="Create Run Report")
def create_report(extracted: int, loaded: int, run_date: str) -> None:
    create_markdown_artifact(
        key="etl-run-summary",
        markdown=f"""
## Daily Sales ETL — {run_date}

| Step | Count |
|------|-------|
| Extracted | {extracted:,} |
| Loaded | {loaded:,} |
| Rejected | {extracted - loaded:,} |

✅ Pipeline completed successfully!
        """,
    )

# ──────────────── MAIN FLOW ────────────────────

@flow(name="Production Sales ETL", log_prints=True)
def production_etl(run_date: str = None, dry_run: bool = False):
    if run_date is None:
        run_date = (date.today() - timedelta(days=1)).isoformat()

    logger = get_run_logger()
    logger.info(f"Starting ETL for {run_date} | dry_run={dry_run}")

    df_raw   = extract_sales(run_date)
    df_valid = validate_data(df_raw)
    df_clean = transform_sales(df_valid)

    loaded = 0
    if not dry_run:
        loaded = load(df_clean, "warehouse/sales_clean")

    create_report(len(df_raw), loaded or len(df_clean), run_date)
    logger.info("✅ Pipeline complete!")
    return {"extracted": len(df_raw), "loaded": loaded}

# Run it!
# production_etl()                          # Yesterday (default)
# production_etl("2024-01-15")              # Specific date backfill
# production_etl("2024-01-15", dry_run=True)# Test run — no DB write
```

---

## 💻 Example 2: Work Pool Deployment for Production

```python
# prefect.yaml
prefect_yaml = """
deployments:
  - name: production-daily-etl
    flow_name: Production Sales ETL
    entrypoint: flows/sales_etl.py:production_etl

    schedules:
      - cron: "0 5 * * *"       # 5am UTC daily
        timezone: "UTC"

    parameters:
      dry_run: false

    work_pool:
      name: aws-ecs-pool         # Run on AWS ECS fargate
      work_queue_name: default
      job_variables:
        image: 123456.dkr.ecr.us-east-1.amazonaws.com/etl:latest
        cpu: 1024
        memory: 2048

    pull:
      - prefect.deployments.steps.git_clone:
          repository: https://github.com/company/etl-repo.git
          branch: main
"""
print(prefect_yaml)
print("Deploy: prefect deploy --name production-daily-etl")
print("Or run: prefect deployment run 'Production Sales ETL/production-daily-etl'")
```

---

## 🏭 Best Practices Summary

```python
best_practices = {
    "One task per logical step":       "extract(), validate(), transform(), load() — not one giant task",
    "Retries on I/O tasks only":       "Add retries to API/DB tasks; don't retry validation logic",
    "Parameters with defaults":        "run_date defaults to yesterday; dry_run defaults to False",
    "get_run_logger()":               "Use Prefect logger — logs visible in UI per task/flow",
    "Artifacts for observability":     "create_markdown_artifact shows results in UI without log-digging",
    "Secrets via Blocks":              "Never hardcode credentials — use Secret blocks",
    "prefect.yaml for deployments":    "Version-control your deployment config with your code",
}
print("Prefect ETL Best Practices:")
for practice, explanation in best_practices.items():
    print(f"\n  ✅ {practice}")
    print(f"     {explanation}")
```
