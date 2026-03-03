# 🎨 Decorators in Python (ETL Context)

---

## 📖 Explanation

A **decorator** is a function that takes another function as input, wraps it with additional behavior, and returns the modified function. It uses the `@` syntax in Python.

Decorators are built on **higher-order functions** (functions that accept/return other functions). In ETL, they are used to add logging, timing, retry logic, caching, and validation without modifying the core logic.

---

## 🧠 Must-Remember Points

- A decorator is just a function that wraps another function: `decorator(func) -> new_func`.
- Always use `@functools.wraps(func)` inside your decorator to preserve the original function's metadata (`__name__`, `__doc__`).
- Decorators with arguments require **three levels of nesting**.
- `@staticmethod`, `@classmethod`, `@property` are built-in decorators.
- `functools.lru_cache` and `functools.cache` are decorators for memoization.
- Decorators are applied **bottom-up** when stacked.
- Class-based decorators are possible using `__call__`.

---

## 💻 Code Examples

### 1️⃣ Basic Decorator Structure

```python
import functools

def my_decorator(func):
    @functools.wraps(func)  # Preserves func metadata
    def wrapper(*args, **kwargs):
        print("Before function call")
        result = func(*args, **kwargs)
        print("After function call")
        return result
    return wrapper

@my_decorator
def say_hello(name):
    """Says hello to someone."""
    print(f"Hello, {name}!")

say_hello("Alice")
# Before function call
# Hello, Alice!
# After function call
print(say_hello.__name__)  # say_hello (not 'wrapper', thanks to @wraps)
```

---

### 2️⃣ ETL: Timing Decorator (Measure ETL Step Duration)

```python
import functools
import time

def timer(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"[TIMER] '{func.__name__}' took {end - start:.4f} seconds")
        return result
    return wrapper

@timer
def extract_data(filepath):
    """Simulates reading a large file."""
    import time
    time.sleep(1.5)  # Simulate I/O
    return [{"id": i, "value": i * 2} for i in range(1000)]

data = extract_data("sales.csv")
# [TIMER] 'extract_data' took 1.5012 seconds
```

---

### 3️⃣ ETL: Logging Decorator

```python
import functools
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def log_etl_step(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"Starting ETL step: {func.__name__}")
        try:
            result = func(*args, **kwargs)
            logging.info(f"Completed ETL step: {func.__name__}")
            return result
        except Exception as e:
            logging.error(f"Failed ETL step: {func.__name__} — Error: {e}")
            raise
    return wrapper

@log_etl_step
def transform_data(data):
    return [row for row in data if row["value"] > 100]

@log_etl_step
def load_data(data, target_db):
    print(f"Loading {len(data)} records into {target_db}")

data = [{"value": 50}, {"value": 200}, {"value": 150}]
transformed = transform_data(data)
load_data(transformed, "PostgreSQL")
```

---

### 4️⃣ ETL: Retry Decorator (Handle Transient Failures)

```python
import functools
import time

def retry(max_attempts=3, delay=2, exceptions=(Exception,)):
    """Decorator with arguments — retries a function on failure."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        raise
                    print(f"Attempt {attempt} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
        return wrapper
    return decorator

@retry(max_attempts=3, delay=1, exceptions=(ConnectionError, TimeoutError))
def connect_to_database(host, port):
    """Simulates a database connection that may fail."""
    import random
    if random.random() < 0.7:  # 70% chance of failure (for demo)
        raise ConnectionError("DB connection refused")
    print(f"Connected to {host}:{port}")

connect_to_database("localhost", 5432)
```

---

### 5️⃣ ETL: Validation Decorator

```python
import functools

def validate_dataframe(func):
    """Ensures the function receives a non-empty pandas DataFrame."""
    @functools.wraps(func)
    def wrapper(df, *args, **kwargs):
        import pandas as pd
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected a DataFrame, got {type(df).__name__}")
        if df.empty:
            raise ValueError("DataFrame is empty — cannot proceed with transformation.")
        return func(df, *args, **kwargs)
    return wrapper

import pandas as pd

@validate_dataframe
def clean_nulls(df, columns):
    return df.dropna(subset=columns)

df = pd.DataFrame({"name": ["Alice", None, "Bob"], "age": [25, 30, None]})
result = clean_nulls(df, columns=["name"])
print(result)
```

---

### 6️⃣ Stacked Decorators

```python
import functools

# Applied bottom-up: timer wraps log, log wraps the function
@timer        # Applied second (outermost)
@log_etl_step # Applied first (innermost)
def extract_from_api(url):
    import time
    time.sleep(0.5)
    return [{"data": "record"}]

extract_from_api("https://api.example.com/data")
# Log fires first, then timer measures total time
```

---

### 7️⃣ `functools.lru_cache` — Memoization Decorator

```python
import functools

# Cache results of expensive lookups
@functools.lru_cache(maxsize=128)
def fetch_reference_data(table_name: str) -> tuple:
    """Fetches and caches reference/lookup table data."""
    print(f"Fetching {table_name} from DB...")
    # Simulated DB call
    data = {"currency": ("USD", "EUR", "GBP"), "status": ("ACTIVE", "INACTIVE")}
    return data.get(table_name, ())

print(fetch_reference_data("currency"))  # Fetches from DB
print(fetch_reference_data("currency"))  # Returns from cache instantly
print(fetch_reference_data.cache_info())  # CacheInfo(hits=1, misses=1, ...)
```

---

## 🏭 ETL Use Cases

| Decorator             | ETL Use Case                                     |
| --------------------- | ------------------------------------------------ |
| `@timer`              | Measure duration of Extract/Transform/Load steps |
| `@log_etl_step`       | Audit trail for each pipeline step               |
| `@retry(...)`         | Retry on API timeouts, DB connection errors      |
| `@validate_dataframe` | Guard against empty/invalid inputs               |
| `@lru_cache`          | Cache lookup/reference table queries             |
| Custom auth decorator | Protect API endpoints that trigger ETL           |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Forgetting @functools.wraps destroys metadata
def bad_decorator(func):
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper  # wrapper.__name__ = 'wrapper', not the original!

# RIGHT: Always use @functools.wraps
def good_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper
```
