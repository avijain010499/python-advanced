# Running dbt with Prefect

A complete guide to orchestrating dbt transformations using Prefect flows — covering both **dbt Core (local CLI)** and **dbt Cloud** approaches.

---

## Why Orchestrate dbt with Prefect?

| Without Prefect | With Prefect |
|---|---|
| Run `dbt run` manually or via cron | Schedule flows with cron, intervals, or event triggers |
| No retry logic | Automatic retries on failure |
| No visibility into task-level status | Full UI observability per task |
| Hard to chain dbt with upstream/downstream steps | Easy to chain: extract → dbt → notify |
| No secrets management | Use Prefect Blocks for credentials |

---

## Approach 1 — dbt Core (CLI via subprocess)

This is the **simplest** approach. Prefect wraps the `dbt` CLI commands using Python's `subprocess` module.

### Prerequisites

```bash
pip install prefect dbt-core dbt-snowflake   # or dbt-postgres, dbt-bigquery etc.
```

---

### Project Structure

```
my_project/
├── dbt_project/          ← your dbt project
│   ├── dbt_project.yml
│   ├── models/
│   └── ...
└── prefect_flow.py       ← your Prefect flow
```

---

### Basic Flow — Run dbt Commands

```python
# prefect_flow.py

import subprocess
from prefect import flow, task

DBT_PROJECT_DIR = "/path/to/dbt_project"

@task(name="dbt seed", retries=2)
def dbt_seed():
    result = subprocess.run(
        ["dbt", "seed", "--project-dir", DBT_PROJECT_DIR],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt seed failed:\n{result.stderr}")

@task(name="dbt run", retries=2)
def dbt_run():
    result = subprocess.run(
        ["dbt", "run", "--project-dir", DBT_PROJECT_DIR],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")

@task(name="dbt test", retries=1)
def dbt_test():
    result = subprocess.run(
        ["dbt", "test", "--project-dir", DBT_PROJECT_DIR],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt test failed:\n{result.stderr}")

@flow(name="dbt Core ETL Flow", log_prints=True)
def dbt_etl_flow():
    dbt_seed()
    dbt_run()
    dbt_test()

if __name__ == "__main__":
    dbt_etl_flow()
```

**Run it:**

```bash
python prefect_flow.py
```

---

### Selective Models — Run Only Specific Models

```python
@task(name="dbt run - staging only")
def dbt_run_staging():
    result = subprocess.run(
        ["dbt", "run", "--select", "staging.*", "--project-dir", DBT_PROJECT_DIR],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr)
```

> **Tip:** The `--select` flag uses dbt's node selection syntax — you can target tags, models, folders, or even upstream/downstream dependencies with `+`.

---

## Approach 2 — prefect-dbt (Recommended for dbt Core)

`prefect-dbt` is the official integration library that adds Prefect tasks and blocks for dbt.

### Installation

```bash
pip install prefect-dbt
```

---

### Flow Using prefect-dbt Core Operations

```python
from prefect import flow
from prefect_dbt.cli.commands import DbtCoreOperation

@flow(name="dbt Core with prefect-dbt", log_prints=True)
def dbt_prefect_flow():
    # dbt seed
    DbtCoreOperation(
        commands=["dbt seed"],
        project_dir="/path/to/dbt_project",
        profiles_dir="/path/to/dbt_project"   # where profiles.yml lives
    ).run()

    # dbt run
    DbtCoreOperation(
        commands=["dbt run"],
        project_dir="/path/to/dbt_project",
        profiles_dir="/path/to/dbt_project"
    ).run()

    # dbt test
    DbtCoreOperation(
        commands=["dbt test"],
        project_dir="/path/to/dbt_project",
        profiles_dir="/path/to/dbt_project"
    ).run()

if __name__ == "__main__":
    dbt_prefect_flow()
```

`DbtCoreOperation` handles:
- Output streaming to Prefect logs
- Non-zero exit code detection
- Optional virtual environment targeting

---

## Approach 3 — dbt Cloud Jobs

Use this if your team already runs dbt Cloud. Prefect can **trigger and monitor** dbt Cloud jobs.

### Installation

```bash
pip install prefect-dbt
```

### Step 1 — Create a Prefect Block for dbt Cloud Credentials

```python
from prefect_dbt.cloud import DbtCloudCredentials

# Run this once to save the block
DbtCloudCredentials(
    api_key="YOUR_DBT_CLOUD_API_KEY",
    account_id=123456   # your dbt Cloud Account ID
).save("my-dbt-cloud-creds")
```

### Step 2 — Run a dbt Cloud Job from a Prefect Flow

```python
from prefect import flow
from prefect_dbt.cloud import DbtCloudJob

@flow(name="Trigger dbt Cloud Job")
def run_dbt_cloud_job():
    dbt_cloud_job = DbtCloudJob(
        dbt_cloud_credentials=DbtCloudCredentials.load("my-dbt-cloud-creds"),
        job_id=123456   # your dbt Cloud Job ID (from the URL)
    )
    dbt_cloud_job.trigger().wait_for_completion()

if __name__ == "__main__":
    run_dbt_cloud_job()
```

> **Where to find your Job ID:** In dbt Cloud, go to **Deploy → Jobs**, click your job, and copy the numeric ID from the URL: `cloud.getdbt.com/deploy/ACCOUNT_ID/projects/PROJECT_ID/jobs/JOB_ID/`.

---

## Full ETL Pipeline — Extract → dbt → Notify

A real-world pattern: chain data extraction, dbt transformations, and Slack notifications.

```python
import subprocess
from prefect import flow, task
from prefect.blocks.system import Secret

DBT_PROJECT_DIR = "/path/to/dbt_project"

@task(name="Extract Data", retries=3, retry_delay_seconds=30)
def extract_data():
    # Your extraction logic here (API call, S3 download, etc.)
    print("Extracting data from source...")

@task(name="dbt run", retries=2)
def dbt_run():
    result = subprocess.run(
        ["dbt", "run", "--project-dir", DBT_PROJECT_DIR],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr)

@task(name="dbt test", retries=1)
def dbt_test():
    result = subprocess.run(
        ["dbt", "test", "--project-dir", DBT_PROJECT_DIR],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr)

@task(name="Send Success Notification")
def notify_success():
    print("✅ Pipeline complete! dbt transformations passed all tests.")

@flow(name="Full ETL + dbt Pipeline", log_prints=True)
def full_etl_pipeline():
    extract_data()
    dbt_run()
    dbt_test()
    notify_success()

if __name__ == "__main__":
    full_etl_pipeline()
```

---

## Scheduling the Flow

### Option A — Schedule with a Deployment (Recommended)

```python
from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

# Deploy with a daily schedule at 6 AM
deployment = Deployment.build_from_flow(
    flow=full_etl_pipeline,
    name="daily-dbt-pipeline",
    schedule=CronSchedule(cron="0 6 * * *", timezone="UTC")
)
deployment.apply()
```

Then start a Prefect worker to execute scheduled runs:

```bash
prefect agent start --pool default-agent-pool
```

### Option B — Serve for Simple Local Scheduling

```python
if __name__ == "__main__":
    full_etl_pipeline.serve(
        name="daily-dbt-pipeline",
        cron="0 6 * * *"
    )
```

---

## Useful dbt CLI Flags

| Flag | Purpose | Example |
|---|---|---|
| `--select` | Run specific models | `dbt run --select staging.*` |
| `--exclude` | Exclude models | `dbt run --exclude marts.revenue` |
| `--target` | Use a specific profile target | `dbt run --target prod` |
| `--vars` | Pass variables to dbt | `dbt run --vars '{"run_date": "2024-01-01"}'` |
| `--full-refresh` | Force full refresh for incremental | `dbt run --full-refresh` |
| `--profiles-dir` | Override profiles.yml location | `dbt run --profiles-dir ~/.dbt` |

---

## Comparison Summary

| Feature | subprocess (DIY) | prefect-dbt Core | dbt Cloud |
|---|---|---|---|
| Setup complexity | Low | Low | Medium |
| Streaming logs | Manual | ✅ Built-in | ✅ Built-in |
| Error detection | Manual | ✅ Automatic | ✅ Automatic |
| Requires dbt Cloud | ❌ | ❌ | ✅ |
| Best for | Quick scripts | Local/dbt Core teams | Teams on dbt Cloud |

---

## Quick Reference Commands

```bash
# Install everything
pip install prefect prefect-dbt dbt-core dbt-snowflake

# Run your flow locally
python prefect_flow.py

# Start Prefect UI (to see runs, logs, schedules)
prefect server start

# Deploy with a schedule
prefect deploy

# Start an agent to run scheduled deployments
prefect agent start --pool default-agent-pool
```
