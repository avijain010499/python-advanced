# 🔔 Logging, Notifications & Alerting in Prefect

---

## 💻 Example 1: Prefect Logging

```python
from prefect import flow, task, get_run_logger

@task(name="Process Data")
def process_data(records: list) -> list:
    logger = get_run_logger()   # Prefect-aware logger — sends logs to UI

    logger.info(f"Starting processing of {len(records):,} records")

    valid = []
    invalid_count = 0
    for record in records:
        if record.get("order_id") and record.get("amount", 0) > 0:
            valid.append(record)
        else:
            invalid_count += 1

    if invalid_count:
        logger.warning(f"Skipped {invalid_count} invalid records")

    logger.info(f"Processing complete: {len(valid):,} valid records")
    return valid

@flow(name="ETL with Logging", log_prints=True)
def logged_etl():
    logger = get_run_logger()
    logger.info("ETL flow started")

    records = [{"order_id": 1, "amount": 100}, {"order_id": None, "amount": 50}]
    clean = process_data(records)

    logger.info(f"ETL complete. Output: {len(clean)} records")
    return len(clean)

# logged_etl()
```

---

## 💻 Example 2: Notifications — Slack, Email, PagerDuty

```python
# Install: pip install prefect-slack
# Configure in Prefect UI: Settings → Automations

from prefect import flow
from prefect.blocks.notifications import SlackWebhook

# First, create the block in Prefect UI or via code:
# SlackWebhook.create(name="etl-alerts", url="https://hooks.slack.com/...")
# Then retrieve it in your flow:

@flow(name="ETL with Slack Alert")
def etl_with_notifications():
    try:
        # Your ETL logic here
        result = run_etl_logic()
        print(f"✅ ETL complete: {result} rows")

    except Exception as e:
        # Send Slack notification on failure
        slack = SlackWebhook.load("etl-alerts")
        slack.notify(
            subject = "❌ ETL Pipeline Failed!",
            body    = f"Flow run failed with error:\n{str(e)}",
        )
        raise   # Re-raise so Prefect marks run as Failed

def run_etl_logic():
    return 1000   # Placeholder

# etl_with_notifications()
print("Configure Slack/email blocks in Prefect UI → Settings → Blocks")
```

---

## 💻 Example 3: Automations — Event-Driven Notifications

```python
# Prefect Automations react to events (no code needed — configured in UI)

automation_examples = """
Configure in Prefect UI → Automations → New Automation:

Trigger: Flow Run State = Failed
Action:  Send Slack message to #data-alerts
Text:    "Flow '{flow_name}' failed after {duration}. View: {flow_run_url}"

---

Trigger: Flow Run State = Failed AND tags contain 'critical'
Action:  Send PagerDuty page

---

Trigger: Flow Run Late (run didn't start within 15 minutes of scheduled time)
Action:  Send email to data-engineering@company.com

---

Trigger: Flow Run Completed (for SLA monitoring)
Action:  Call webhook to update dashboard status page
"""
print(automation_examples)
```

---

## 💻 Example 4: Custom Logging for ETL Observability

```python
from prefect import flow, task, get_run_logger
from datetime import datetime

class ETLMetrics:
    """Track metrics throughout the ETL run."""
    def __init__(self):
        self.extracted_rows = 0
        self.valid_rows = 0
        self.loaded_rows = 0
        self.start_time = datetime.utcnow()

    def summary(self) -> dict:
        duration = (datetime.utcnow() - self.start_time).total_seconds()
        return {
            "extracted": self.extracted_rows,
            "valid":     self.valid_rows,
            "loaded":    self.loaded_rows,
            "rejected":  self.extracted_rows - self.valid_rows,
            "duration_s": round(duration, 2),
        }

@flow(name="Observable ETL", log_prints=True)
def observable_etl(run_date: str = "2024-01-15"):
    logger  = get_run_logger()
    metrics = ETLMetrics()

    # Simulate ETL
    raw_records = list(range(1000))   # 1000 raw records
    metrics.extracted_rows = len(raw_records)
    logger.info(f"EXTRACT: {metrics.extracted_rows:,} rows from source")

    valid = [r for r in raw_records if r > 50]  # Filter
    metrics.valid_rows = len(valid)
    logger.info(f"TRANSFORM: {metrics.valid_rows:,} valid rows ({metrics.extracted_rows - metrics.valid_rows} rejected)")

    metrics.loaded_rows = metrics.valid_rows
    logger.info(f"LOAD: {metrics.loaded_rows:,} rows written to target")

    summary = metrics.summary()
    logger.info(f"ETL SUMMARY: {summary}")
    return summary

# result = observable_etl()
# print(result)
```

---

## 🏭 Summary

| Feature               | Use Case                                            |
| --------------------- | --------------------------------------------------- |
| `get_run_logger()`    | Structured logs visible in Prefect UI per task/flow |
| `log_prints=True`     | Capture `print()` calls as Prefect logs             |
| `SlackWebhook.load()` | Send failure alerts to Slack channel                |
| Automations           | Rule-based alerts without code — in Prefect UI      |
| Custom metrics class  | Track extract/validate/load counts across tasks     |
