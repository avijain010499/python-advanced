# 🔧 Flows, Tasks & Task Configuration in Prefect

---

## 🤔 Flows vs Tasks — When to Use Which?

- **`@flow`**: The top-level pipeline. Calls tasks in order. Can call other flows (subflows).
- **`@task`**: A single unit of work — one API call, one transformation, one file write.

> 💡 **Rule**: If it can fail independently and be retried, it should be a `@task`. If it orchestrates multiple tasks, it should be a `@flow`.

---

## 💻 Example 1: Task Configuration Options

```python
from prefect import task, flow
from datetime import timedelta
import requests, pandas as pd

@task(
    name         = "Fetch Sales from API",     # Display name in UI
    description  = "Calls the sales REST API and returns JSON data",
    retries      = 3,                          # Retry 3 times on failure
    retry_delay_seconds = 30,                  # Wait 30s between retries
    timeout_seconds     = 120,                 # Fail if takes > 2 minutes
    tags         = ["api", "extract"],         # Group/filter in UI
    log_prints   = True,                       # Capture print() as logs
)
def fetch_sales_api(date: str) -> list:
    """Fetch sales records for a given date from the REST API."""
    url = f"https://api.company.com/sales?date={date}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()   # Raises error on 4xx/5xx → triggers retry!
    data = response.json()
    print(f"Fetched {len(data):,} records for {date}")
    return data

@task(name="Validate Data", retries=0)
def validate_data(records: list) -> list:
    """Remove records with missing order_id or negative amounts."""
    valid = [r for r in records if r.get("order_id") and r.get("amount", 0) > 0]
    print(f"Valid: {len(valid):,} / Total: {len(records):,}")
    return valid

@flow(name="Sales ETL Pipeline", log_prints=True)
def sales_etl(date: str = "2024-01-15"):
    records   = fetch_sales_api(date)
    validated = validate_data(records)
    print(f"Pipeline complete for {date}: {len(validated):,} clean records")
    return len(validated)

# Run locally
# sales_etl("2024-01-15")
```

---

## 💻 Example 2: Task Dependencies & Parallel Execution

```python
from prefect import flow, task
import time

@task
def extract_sales() -> str:
    time.sleep(1)
    return "sales_data"

@task
def extract_customers() -> str:
    time.sleep(1)       # Both run ~simultaneously in parallel!
    return "customer_data"

@task
def extract_products() -> str:
    time.sleep(1)
    return "product_data"

@task
def join_and_load(sales, customers, products) -> None:
    print(f"Joining: {sales} + {customers} + {products}")

@flow(name="Parallel Extract ETL")
def parallel_etl():
    # These 3 tasks submit immediately — Prefect runs them in parallel
    sales     = extract_sales.submit()      # .submit() = async, returns Future
    customers = extract_customers.submit()
    products  = extract_products.submit()

    # This task waits for all 3 to complete before running
    join_and_load(
        sales.result(),       # .result() blocks until the task is done
        customers.result(),
        products.result(),
    )

# parallel_etl()   # Total time ~1s, not ~3s!
print("Use .submit() for parallel tasks, .result() to wait for output")
```

---

## 💻 Example 3: Conditional Task Execution

```python
from prefect import flow, task
from prefect.context import get_run_context

@task
def extract(source: str) -> list:
    print(f"Extracting from {source}")
    return [{"id": 1}, {"id": 2}]

@task
def validate(records: list) -> bool:
    is_valid = len(records) > 0
    print(f"Validation: {'passed' if is_valid else 'FAILED'}")
    return is_valid

@task
def load(records: list) -> None:
    print(f"Loading {len(records)} records")

@task
def send_alert(message: str) -> None:
    print(f"🚨 ALERT: {message}")

@flow(name="Conditional ETL")
def conditional_etl(source: str = "sales_api"):
    records = extract(source)
    is_valid = validate(records)

    if is_valid:
        load(records)
    else:
        send_alert(f"Validation failed for {source} — pipeline aborted!")

# conditional_etl()
```

---

## 💻 Example 4: Task Caching — Skip Redundant Runs

```python
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(
    cache_key_fn      = task_input_hash,   # Cache based on input parameters
    cache_expiration  = timedelta(hours=1), # Cache valid for 1 hour
    name              = "Expensive API Call",
)
def fetch_exchange_rates(base_currency: str) -> dict:
    """Fetch exchange rates — cached so we don't call the API every minute."""
    import requests
    response = requests.get(f"https://api.exchangerate.host/latest?base={base_currency}")
    rates = response.json()["rates"]
    print(f"Fetched {len(rates)} exchange rates")
    return rates

@flow
def etl_with_caching():
    rates1 = fetch_exchange_rates("USD")  # Calls API
    rates2 = fetch_exchange_rates("USD")  # Uses CACHE — no API call!
    rates3 = fetch_exchange_rates("EUR")  # Different input → calls API again
    print("Done")

# etl_with_caching()
print("Caching = skip redundant task runs within the cache_expiration window")
```

---

## ⚠️ Common Mistakes

```python
mistakes = [
    ("Calling .result() before submitting all tasks",
     "Submit all tasks first (.submit()), then call .result() → enables parallelism"),
    ("No retries on API/network tasks",
     "APIs fail transiently — always add retries=3 for external calls"),
    ("Heavy computation in @flow body",
     "Put computation in @task functions — flows should only orchestrate"),
]
for mistake, fix in mistakes:
    print(f"❌ {mistake}")
    print(f"✅ Fix: {fix}\n")
```
