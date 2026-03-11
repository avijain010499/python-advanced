# ⏰ Scheduling in Prefect

---

## 🤔 What Is Scheduling?

Scheduling makes your flows run **automatically** at specific times — without needing to manually trigger them. Prefect supports cron expressions, interval-based schedules, and RRule schedules.

---

## 💻 Example 1: Schedule Types

```python
from prefect import flow
from prefect.schedules import CronSchedule, IntervalSchedule, RRuleSchedule
from datetime import timedelta

# 1. Cron Schedule — most flexible
@flow(name="Daily ETL")
def daily_etl():
    print("Running daily ETL...")

# Cron expressions:
cron_examples = {
    "0 5 * * *":     "Every day at 5:00 AM UTC",
    "0 */6 * * *":   "Every 6 hours",
    "0 8 * * 1-5":   "Weekdays at 8am",
    "0 0 1 * *":     "First day of every month at midnight",
    "*/15 * * * *":  "Every 15 minutes",
}
print("Common cron expressions:")
for cron, desc in cron_examples.items():
    print(f"  {cron:<20} → {desc}")

# 2. Interval Schedule
interval_schedule = IntervalSchedule(interval=timedelta(hours=6))
print(f"\nInterval: {interval_schedule}")

# 3. RRule (complex) - e.g., every Monday and Thursday at 9am
print("RRule: FREQ=WEEKLY;BYDAY=MO,TH;BYHOUR=9;BYMINUTE=0")
```

---

## 💻 Example 2: Creating a Deployment with a Schedule

```python
# Deployments link your flow to a schedule and work pool
# Run these commands in your terminal:

deployment_commands = """
# Method 1: Using CLI
prefect deployment build daily_etl.py:daily_etl \\
    --name "production-daily" \\
    --cron "0 5 * * *" \\
    --timezone "UTC" \\
    --work-queue default

prefect deployment apply daily_etl-deployment.yaml
prefect deployment run "Daily ETL/production-daily"   # Manual trigger

# Method 2: prefect.yaml file (recommended for production)
"""
print(deployment_commands)

# prefect.yaml file content:
prefect_yaml = """
# prefect.yaml — defines all deployments for your project
deployments:
  - name: daily-sales-etl
    flow_name: Daily Sales ETL
    entrypoint: flows/sales_etl.py:daily_sales_etl

    schedules:
      - cron: "0 5 * * *"
        timezone: "UTC"
        active: true

    parameters:
      input_path: "s3://my-bucket/raw/sales/"
      output_path: "s3://my-bucket/clean/sales/"

    work_pool:
      name: my-work-pool
"""
print(prefect_yaml)
print("Apply: prefect deploy --all")
```

---

## 💻 Example 3: Parameterized Scheduled Flows

```python
from prefect import flow, task
from datetime import date, timedelta

@task
def get_yesterday() -> str:
    return (date.today() - timedelta(days=1)).isoformat()

@flow(name="Incremental Sales ETL", log_prints=True)
def incremental_etl(
    run_date: str = None,    # If None, defaults to yesterday
    source: str = "snowflake",
    dry_run: bool = False,
):
    """
    Parameterized flow:
    - Scheduled runs use default run_date (yesterday)
    - Manual runs can override all parameters
    """
    if run_date is None:
        run_date = get_yesterday()

    print(f"Processing date: {run_date}")
    print(f"Source: {source}, dry_run={dry_run}")

    if dry_run:
        print("🟡 Dry run mode — not writing to destination")
        return

    # ... actual ETL logic here

# Scheduled: uses defaults (yesterday, snowflake, no dry_run)
# Manual backfill: incremental_etl(run_date="2024-01-01")
# Test run:        incremental_etl(dry_run=True)
# incremental_etl()
```

---

## 💻 Example 4: Pause and Resume Schedules

```python
# Manage schedules via CLI

schedule_management = """
# Pause a deployment's schedule (stop automatic runs)
prefect deployment pause-schedule "Daily Sales ETL/daily-sales-etl"

# Resume a paused schedule
prefect deployment resume-schedule "Daily Sales ETL/daily-sales-etl"

# List all deployments and their schedule status
prefect deployment ls

# Trigger an immediate run (ignores schedule)
prefect deployment run "Daily Sales ETL/daily-sales-etl" \\
    --param run_date=2024-01-15 \\
    --param dry_run=true

# Cancel a running flow run
prefect flow-run cancel <run-id>

# View recent flow runs
prefect flow-run ls --limit 10
"""
print(schedule_management)
```

---

## 🏭 Scheduling Summary

| Schedule Type      | Use For                                             |
| ------------------ | --------------------------------------------------- |
| `CronSchedule`     | Precise time-based: daily at 5am, weekdays, monthly |
| `IntervalSchedule` | Frequency-based: every 6 hours, every 30 minutes    |
| `timedelta`        | Simple repeat: every N hours/minutes                |
| `prefect.yaml`     | Full deployment config in version-controlled file   |
| `--param` in CLI   | Override defaults for manual/backfill runs          |

---

## ⚠️ Common Mistakes

```python
mistakes = [
    ("No timezone in cron schedule", "Default is UTC — production cron misses DST shifts"),
    ("Hardcoding dates in flows", "Use parameters with defaults = yesterday's date"),
    ("Forgetting to apply deployment after changes", "prefect deploy re-registers the schedule"),
]
for m, fix in mistakes:
    print(f"❌ {m}")
    print(f"✅ {fix}\n")
```
