# 🗄️ Results, Artifacts & Storage in Prefect

---

## 💻 Example 1: Prefect Artifacts — Record Results in UI

```python
from prefect import flow, task
from prefect.artifacts import create_table_artifact, create_markdown_artifact, create_link_artifact
import pandas as pd

@task(name="Generate ETL Report")
def generate_report(df: pd.DataFrame) -> None:
    """Create rich artifacts visible in the Prefect UI after run completes."""

    # 1. Table artifact — shows a formatted table in UI
    summary = df.groupby("region")["revenue"].agg(["sum","count","mean"]).reset_index()
    create_table_artifact(
        key     = "revenue-by-region",      # Appears in Artifacts tab in UI
        table   = summary.to_dict("records"),
        description = "Revenue summary grouped by region",
    )

    # 2. Markdown artifact — formatted text report
    total    = df["revenue"].sum()
    top_region = df.groupby("region")["revenue"].sum().idxmax()
    create_markdown_artifact(
        key     = "etl-run-summary",
        markdown = f"""
## ETL Run Summary

| Metric | Value |
|--------|-------|
| Total Revenue | ${total:,.2f} |
| Top Region    | {top_region} |
| Records Loaded| {len(df):,} |

✅ Pipeline completed successfully.
        """,
        description = "Human-readable ETL run summary",
    )

    # 3. Link artifact — link to external dashboard or file
    create_link_artifact(
        key         = "output-location",
        link        = "s3://my-bucket/output/2024-01-15/",
        description = "S3 path where output Parquet was written",
    )

@flow(name="ETL with Artifacts", log_prints=True)
def etl_with_artifacts():
    # Simulate ETL result
    df = pd.DataFrame({
        "region":  ["NORTH","SOUTH","EAST","NORTH","WEST"],
        "revenue": [1500, 800, 1200, 2000, 600],
    })
    generate_report(df)
    print("Artifacts created — view in Prefect UI under this flow run")

# etl_with_artifacts()
```

---

## 💻 Example 2: Configure Result Storage — Persist Task Outputs

```python
from prefect import flow, task
from prefect.results import LocalFileSystemResultStorage
import pandas as pd

# Results: persist task return values so they can be retrieved later
# Useful for: debugging, resuming failed runs, downstream consumption

@task(
    name              = "Expensive Transform",
    result_storage    = LocalFileSystemResultStorage(basepath="/tmp/prefect_results"),
    persist_result    = True,    # Save result to storage
    cache_key_fn      = lambda ctx, params: f"transform_{params['date']}",
    cache_expiration  = None,    # Cache forever (for this date)
)
def expensive_transform(records: list, date: str) -> list:
    """This takes 5 minutes to run — cache the result!"""
    import time; time.sleep(0.1)   # Simulating expensive work
    return [r for r in records if r > 0]

@flow(name="Result Storage Demo", persist_result=True)
def demo_with_results():
    records = [1, -2, 3, -4, 5]
    result  = expensive_transform(records, date="2024-01-15")
    print(f"Transformed: {result}")
    return result

# demo_with_results()
print("With persist_result=True, results survive between flow runs")
```

---

## 💻 Example 3: Store Results in S3

```python
# pip install prefect-aws
from prefect.filesystems import S3
from prefect import flow, task
import pandas as pd

# Register S3 block once:
# S3(bucket_path="my-bucket/prefect-results/").save("my-s3-results")

@task(
    persist_result = True,
    result_storage = S3.load("my-s3-results"),
)
def transform_large_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Result saved to S3 — survives crashes, can be inspected anytime."""
    df["revenue"] = df["qty"] * df["unit_price"]
    return df.dropna()

@flow(name="S3 Result Storage")
def pipeline_with_s3_results():
    df = pd.DataFrame({
        "qty": [5, 10, None],
        "unit_price": [100, 200, 150],
    })
    result = transform_large_dataset(df)
    print(f"Result saved to S3: {len(result)} rows")

# pipeline_with_s3_results()
print("Results in S3 = retrieve them hours/days later for debugging")
```

---

## 🏭 Summary

| Feature                    | Use Case                                            |
| -------------------------- | --------------------------------------------------- |
| `create_table_artifact`    | Show aggregated results in Prefect UI               |
| `create_markdown_artifact` | Human-readable run summary                          |
| `create_link_artifact`     | Link to output file location                        |
| `persist_result=True`      | Save task output to disk/S3 for debugging           |
| `cache_key_fn`             | Skip re-running expensive tasks if result is cached |

---

## ⚠️ Common Mistakes

```python
mistakes = [
    ("No artifacts", "Without artifacts, you can't see what each run produced in the UI"),
    ("Caching without cache_key_fn", "Default cache key = hash of all args — be explicit"),
    ("persist_result without storage", "Defaults to local filesystem — lost after container restart"),
]
for m, fix in mistakes:
    print(f"❌ {m}")
    print(f"✅ Fix: Use explicit result_storage pointing to S3/GCS for production\n")
```
