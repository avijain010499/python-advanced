# 🔁 Retries, Error Handling & States in Prefect

---

## 💻 Example 1: Retry Configuration

```python
from prefect import flow, task
from datetime import timedelta
import random

@task(
    name                = "Fetch from API",
    retries             = 3,               # Try 3 more times on failure
    retry_delay_seconds = 30,              # Wait 30s between retries
    # Exponential backoff: first retry=30s, second=60s, third=120s
    retry_jitter_factor = 0.2,             # Add ±20% jitter to avoid thundering herd
)
def fetch_from_api(endpoint: str) -> dict:
    if random.random() < 0.6:   # 60% chance of failure (for demonstration)
        raise ConnectionError(f"API {endpoint} timed out")
    return {"data": [1, 2, 3]}

@task(
    retries             = 5,
    retry_delay_seconds = exponential_backoff(backoff_factor=2),
    # Retry after: 2s, 4s, 8s, 16s, 32s
)
def flaky_db_write(records: list) -> None:
    if random.random() < 0.3:
        raise Exception("DB connection pool exhausted")
    print(f"Written {len(records)} records")

from prefect.tasks import exponential_backoff

@task(retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=2))
def resilient_write(records) -> None:
    print(f"Writing {len(records)} records")

@flow(name="Resilient ETL")
def resilient_etl():
    data = fetch_from_api("https://api.company.com/sales")
    resilient_write(data.get("data", []))

# resilient_etl()
```

---

## 💻 Example 2: Task States and Their Meanings

```python
from prefect import flow, task
from prefect.states import Failed, Completed, Cancelled

@task
def risky_task(will_fail: bool) -> str:
    if will_fail:
        raise ValueError("Task failed deliberately")
    return "success"

@flow(name="State Demo")
def state_demo():
    # Get full state info from task
    state = risky_task(will_fail=False, return_state=True)
    print(f"State type: {type(state).__name__}")  # Completed
    print(f"Is complete: {state.is_completed()}")   # True
    print(f"Result: {state.result()}")              # "success"

    state2 = risky_task(will_fail=True, return_state=True)
    print(f"Is failed: {state2.is_failed()}")       # True
    # Accessing result of failed task raises the original exception:
    # state2.result()  → raises ValueError

state_types = {
    "Pending":   "Task submitted but not yet running",
    "Running":   "Task currently executing",
    "Completed": "Task finished successfully",
    "Failed":    "Task raised an exception (after all retries)",
    "Crashed":   "Task process died unexpectedly",
    "Cancelled": "Task was manually cancelled",
    "Cached":    "Task used cached result — did not re-run",
}
print("Task States:")
for state, desc in state_types.items():
    print(f"  {state:<12} → {desc}")
```

---

## 💻 Example 3: On-Failure Hooks

```python
from prefect import flow, task
from prefect.states import Failed
import traceback

def on_flow_failure(flow, flow_run, state):
    """Called automatically when a flow run fails."""
    print(f"🚨 Flow '{flow.name}' FAILED!")
    print(f"   Run ID: {flow_run.id}")
    print(f"   State:  {state.message}")
    # Here: send alert, record to database, page on-call, etc.

def on_task_failure(task, task_run, state):
    """Called when a specific task fails."""
    print(f"⚠️ Task '{task.name}' failed in run {task_run.id}")

@task(on_failure=[on_task_failure])
def critical_load(records: list) -> None:
    if not records:
        raise ValueError("Empty dataset — cannot load!")
    print(f"Loaded {len(records)} records")

@flow(
    name       = "ETL with Error Hooks",
    on_failure = [on_flow_failure],
    log_prints = True,
)
def etl_with_hooks():
    critical_load([])   # This will fail and trigger both hooks

# etl_with_hooks()
```

---

## 💻 Example 4: Safe Task Calls — Crash Isolation

```python
from prefect import flow, task

@task(retries=2)
def load_to_primary(records: list) -> bool:
    print("Loading to primary DB...")
    # raise Exception("Primary DB down!")   # Uncomment to simulate failure
    return True

@task
def load_to_backup(records: list) -> bool:
    print("Loading to backup DB...")
    return True

@flow(name="Resilient Load with Fallback")
def load_with_fallback(records: list):
    primary_state = load_to_primary.submit(records, return_state=True)

    if not primary_state.result().is_completed():
        print("⚠️ Primary load failed! Falling back to backup...")
        backup_ok = load_to_backup(records)
        return {"destination": "backup", "success": backup_ok}
    return {"destination": "primary", "success": True}

# load_with_fallback([1, 2, 3])
```

---

## 🏭 Summary

| Feature                          | Use Case                                               |
| -------------------------------- | ------------------------------------------------------ |
| `retries=3`                      | Network failures, API rate limits, transient DB errors |
| `exponential_backoff`            | Avoid overwhelming external services on retry          |
| `return_state=True`              | Inspect task state before deciding next step           |
| `on_failure=[hook]`              | Trigger alerts or cleanup on failure                   |
| `is_completed()` / `is_failed()` | Conditional flow branching based on task outcome       |
