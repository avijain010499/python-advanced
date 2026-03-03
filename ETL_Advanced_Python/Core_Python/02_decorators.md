# 🎨 Decorators in Python (ETL Context)

---

## 🤔 What Is a Decorator and Why Should You Care?

Imagine you write an ETL function that reads data from a database. After writing it, you realize you also want to:

- **Log** when it starts and ends
- **Time** how long it takes
- **Retry** automatically if the connection fails

You _could_ copy-paste the timing/logging/retry code into every function. But that would be messy, repetitive, and hard to maintain.

**Decorators** let you wrap a function with extra behavior — cleanly and reusably — without changing the function's own code.

> 💡 **Real-world analogy**: Think of a decorator like a **gift wrapper**. The gift (your function) stays the same inside. The wrapping (the decorator) adds presentation and extra features on the outside. You can use the same wrapping paper on any gift!

---

## 🧱 Building Blocks You Need to Know First

Before understanding decorators, you need to know two things:

### Functions are objects in Python

```python
# Functions can be assigned to variables
def greet(name):
    return f"Hello, {name}!"

# 'greet' is just a variable pointing to the function object
say_hi = greet           # 'say_hi' now points to the same function
print(say_hi("Alice"))   # Hello, Alice!   — works perfectly!

# Functions can be passed to other functions
def run_twice(func, value):
    func(value)
    func(value)

run_twice(greet, "Bob")  # Calls greet("Bob") twice
```

### Functions can return other functions

```python
def make_greeting(greeting_word):
    """This function CREATES and RETURNS a new function."""

    def greet(name):
        return f"{greeting_word}, {name}!"

    return greet          # Return the function itself (not the result of calling it)


hello_greeter = make_greeting("Hello")
hi_greeter    = make_greeting("Hi there")

print(hello_greeter("Alice"))   # Hello, Alice!
print(hi_greeter("Bob"))        # Hi there, Bob!
```

---

## 🧱 How Decorators Work (Step by Step)

A decorator is a function that:

1. Takes a function as an argument
2. Creates a **wrapper** function that adds behavior before/after calling the original
3. Returns the wrapper function

```python
# ---- Step 1: Define the decorator ----
def my_decorator(func):
    """
    'func' is the function being decorated.
    This decorator adds behavior BEFORE and AFTER calling func.
    """

    def wrapper(*args, **kwargs):
        """
        This is the wrapper function.
        *args and **kwargs capture ALL arguments so we can pass them through.
        """
        print("🔵 Before the function runs")

        result = func(*args, **kwargs)   # Call the original function

        print("🟢 After the function runs")
        return result                    # Return whatever the original returned

    return wrapper   # Return the wrapper, not the result of calling it!


# ---- Step 2: Apply the decorator ----
# Method A: The @ syntax (most common — this is just shorthand for Method B)
@my_decorator
def say_hello(name):
    print(f"Hello, {name}!")

# Method B: The manual way (equivalent to Method A)
def say_hello_v2(name):
    print(f"Hello, {name}!")
say_hello_v2 = my_decorator(say_hello_v2)   # This is what @my_decorator actually does!

# ---- Step 3: Call it ----
say_hello("Alice")
# 🔵 Before the function runs
# Hello, Alice!
# 🟢 After the function runs
```

---

## 🔧 `functools.wraps` — Always Use This!

Without `@functools.wraps`, your decorator hides the original function's identity:

```python
import functools

# ❌ Without @wraps
def bad_decorator(func):
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

@bad_decorator
def my_function():
    """This is the docstring of my_function."""
    pass

print(my_function.__name__)   # 'wrapper'  ← WRONG! Should be 'my_function'
print(my_function.__doc__)    # None        ← WRONG! Docstring is lost!


# ✅ With @functools.wraps — fixes the identity problem
def good_decorator(func):
    @functools.wraps(func)       # Copies __name__, __doc__, etc. from 'func'
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

@good_decorator
def my_function():
    """This is the docstring of my_function."""
    pass

print(my_function.__name__)   # 'my_function'  ✅
print(my_function.__doc__)    # 'This is the docstring of my_function.'  ✅
```

---

## 💻 ETL Code Examples

### Example 1: Timer Decorator — Measure ETL Step Duration

```python
import functools
import time

def timer(func):
    """
    Decorator that measures and prints how long a function takes to run.

    WHY THIS MATTERS IN ETL:
    When a pipeline is slow, you need to know WHICH step is the bottleneck.
    Adding @timer to each step gives you precise timing data automatically.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Record the start time (perf_counter is more precise than time.time)
        start = time.perf_counter()

        # Run the actual function
        result = func(*args, **kwargs)

        # Record end time and calculate duration
        end = time.perf_counter()
        duration = end - start

        # Report the timing
        print(f"⏱️  [{func.__name__}] completed in {duration:.4f} seconds")
        return result

    return wrapper


# Apply the decorator to your ETL functions
@timer
def extract_data(source_file):
    """Read all records from a CSV source file."""
    import time
    time.sleep(1.2)   # Simulating a real file read (slow I/O)
    return [{"id": i, "value": i * 100} for i in range(10000)]

@timer
def transform_data(records):
    """Filter records and apply business logic."""
    time.sleep(0.4)   # Simulating processing time
    return [r for r in records if r["value"] > 500000]

@timer
def load_data(records, target):
    """Insert records into the target database."""
    time.sleep(0.8)   # Simulating DB writes
    print(f"  Loaded {len(records)} records into '{target}'")


# Run the ETL pipeline — timing is automatic!
raw_data      = extract_data("sales.csv")
clean_data    = transform_data(raw_data)
load_data(clean_data, "fact_sales")

# ⏱️  [extract_data] completed in 1.2003 seconds
# ⏱️  [transform_data] completed in 0.4001 seconds
# ⏱️  [load_data] completed in 0.8002 seconds
```

---

### Example 2: Logging Decorator — Audit Trail for Every Step

```python
import functools
import logging
from datetime import datetime

# Set up logging (see file 10_logging.md for full details)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def log_etl_step(func):
    """
    Decorator that logs the start, end, and any error for an ETL function.

    WHY THIS MATTERS IN ETL:
    ETL jobs often run at night unattended. Logging every step creates an
    audit trail so you can see exactly what happened if something goes wrong.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Log the start
        logging.info(f"▶  Starting: {func.__name__}")

        try:
            result = func(*args, **kwargs)
            # Log success
            logging.info(f"✅ Completed: {func.__name__}")
            return result

        except Exception as e:
            # Log the failure with the error message
            logging.error(f"❌ FAILED: {func.__name__} — {type(e).__name__}: {e}")
            raise   # Re-raise so the error still propagates

    return wrapper


@log_etl_step
def extract_from_api(url):
    """Fetch data from an external REST API."""
    # Simulating an API call
    return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

@log_etl_step
def transform_records(records):
    """Apply business transformations."""
    return [{**r, "name": r["name"].upper()} for r in records]    # uppercase names

@log_etl_step
def load_to_database(records, table_name):
    """Load transformed records into the target database."""
    print(f"  → Loaded {len(records)} records into '{table_name}'")

data   = extract_from_api("https://api.example.com/employees")
result = transform_records(data)
load_to_database(result, "dim_employees")

# 2024-01-15 10:30:00 | INFO | ▶  Starting: extract_from_api
# 2024-01-15 10:30:00 | INFO | ✅ Completed: extract_from_api
# 2024-01-15 10:30:00 | INFO | ▶  Starting: transform_records
# ...
```

---

### Example 3: Retry Decorator — Handle Temporary Failures

```python
import functools
import time
import logging

def retry(max_attempts=3, delay_seconds=2, exceptions=(Exception,)):
    """
    Decorator FACTORY — a decorator that takes its own arguments!

    WHY THREE LEVELS?
    - retry(max_attempts=3) is called first → returns a decorator
    - The decorator takes 'func' as argument → returns wrapper
    - wrapper is what actually runs when you call the decorated function

    WHY THIS MATTERS IN ETL:
    Database connections, API calls, and network requests sometimes fail
    temporarily (server restart, network blip, rate limits). Retrying
    automatically makes your pipeline resilient without manual intervention.

    Arguments:
    - max_attempts: How many times to try before giving up
    - delay_seconds: How long to wait between attempts (in seconds)
    - exceptions: Which exception types to retry on (others fail immediately)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)     # Try the function
                    if attempt > 1:
                        logging.info(f"✅ {func.__name__} succeeded on attempt {attempt}")
                    return result                       # Return on success

                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        logging.warning(
                            f"⚠️  Attempt {attempt}/{max_attempts} failed: {e}. "
                            f"Retrying in {delay_seconds}s..."
                        )
                        time.sleep(delay_seconds)      # Wait before retrying
                    else:
                        logging.error(
                            f"❌ All {max_attempts} attempts failed for {func.__name__}"
                        )

            raise last_exception   # Re-raise the last error if all attempts fail

        return wrapper
    return decorator


# Apply with specific retry settings
@retry(max_attempts=3, delay_seconds=1, exceptions=(ConnectionError, TimeoutError))
def connect_to_database(host, port, dbname):
    """
    Establish a connection to the PostgreSQL database.
    Retries 3 times if ConnectionError or TimeoutError occurs.
    """
    import random
    # Simulate random connection failures (in real code, this would be psycopg2.connect)
    if random.random() < 0.6:   # 60% chance of failure (for demonstration)
        raise ConnectionError(f"Cannot connect to {host}:{port}/{dbname}")
    print(f"✅ Connected to {host}:{port}/{dbname}")
    return {"conn": "active"}

try:
    conn = connect_to_database("localhost", 5432, "etl_db")
except ConnectionError:
    print("❌ Pipeline cannot continue — database unavailable after 3 attempts.")
```

---

### Example 4: Validation Decorator — Guard Input Data

```python
import functools
import pandas as pd

def validate_dataframe(required_columns=None):
    """
    Decorator that checks a DataFrame argument before the function runs.

    Ensures:
    1. The first argument is actually a pandas DataFrame
    2. The DataFrame is not empty
    3. All required columns are present

    WHY THIS MATTERS IN ETL:
    ETL transform functions often assume clean input. If upstream sends an
    empty DataFrame or missing columns, a cryptic KeyError deep inside your
    function is hard to debug. This decorator catches it at the entry point
    with a clear, descriptive message.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(df, *args, **kwargs):
            # Check type
            if not isinstance(df, pd.DataFrame):
                raise TypeError(
                    f"'{func.__name__}' expects a pandas DataFrame as the first argument, "
                    f"but got {type(df).__name__}"
                )

            # Check not empty
            if df.empty:
                raise ValueError(
                    f"'{func.__name__}' received an EMPTY DataFrame. "
                    f"Nothing to process."
                )

            # Check required columns exist
            if required_columns:
                missing = set(required_columns) - set(df.columns)
                if missing:
                    raise ValueError(
                        f"'{func.__name__}' is missing required columns: {missing}. "
                        f"Available columns: {list(df.columns)}"
                    )

            return func(df, *args, **kwargs)

        return wrapper
    return decorator


@validate_dataframe(required_columns=["customer_id", "amount", "status"])
def clean_sales_data(df, min_amount=0):
    """Transform and clean the sales DataFrame."""
    df = df[df["amount"] >= min_amount]
    df = df[df["status"] == "COMPLETED"]
    return df.reset_index(drop=True)


# This will work fine:
df_good = pd.DataFrame({
    "customer_id": [1, 2, 3],
    "amount": [500, 150, 1200],
    "status": ["COMPLETED", "PENDING", "COMPLETED"]
})
result = clean_sales_data(df_good, min_amount=100)
print(result)

# This will raise a clear error:
df_bad = pd.DataFrame({"customer_id": [1], "amount": [100]})  # Missing 'status'
# clean_sales_data(df_bad)  # → ValueError: missing required columns: {'status'}
```

---

### Example 5: Stacking Multiple Decorators

```python
# You can apply multiple decorators to the same function.
# They are applied BOTTOM-UP (the one closest to the function is applied first).

import functools
import time
import logging

# @log_etl_step decorates FIRST (inner)
# @timer decorates SECOND (outer — wraps the logged version)
@timer           # Applied second (runs second — outer wrapper)
@log_etl_step    # Applied first  (runs first  — inner wrapper)
def extract_from_database(query, limit=None):
    """Extracts records from the database using the provided SQL query."""
    time.sleep(0.5)   # Simulate DB query time
    return [{"id": i} for i in range(limit or 100)]

data = extract_from_database("SELECT * FROM sales", limit=500)
# Log fires: "Starting: extract_from_database"
# Function runs
# Log fires: "Completed: extract_from_database"
# Timer fires: "[extract_from_database] completed in 0.5003 seconds"
```

---

### Example 6: `functools.lru_cache` — Cache Expensive Lookups

```python
import functools

# The @lru_cache decorator is built into Python's functools module.
# 'lru' stands for "Least Recently Used" — it evicts the oldest unused results.
# maxsize=128 means it caches up to 128 unique results.

@functools.lru_cache(maxsize=128)
def fetch_department_name(dept_id: int) -> str:
    """
    Look up a department name by ID.

    WHY CACHE THIS?
    In ETL, you might process 1 million orders, each needing a department lookup.
    Instead of 1 million database calls, the cache returns stored results instantly
    after the first lookup for each unique dept_id.
    """
    print(f"  → Database call for dept_id={dept_id} (expensive!)")
    # Simulating a database call
    departments = {10: "Information Technology", 20: "Human Resources", 30: "Finance"}
    return departments.get(dept_id, "Unknown Department")


# First calls hit the database
print(fetch_department_name(10))   # → Database call... "Information Technology"
print(fetch_department_name(20))   # → Database call... "Human Resources"

# Repeated calls use the cache (NO database call)
print(fetch_department_name(10))   # From cache instantly: "Information Technology"
print(fetch_department_name(10))   # From cache instantly: "Information Technology"

# Check cache statistics
print(fetch_department_name.cache_info())
# CacheInfo(hits=2, misses=2, maxsize=128, currsize=2)
# hits=2 means 2 calls were served from cache (saved 2 DB queries!)
```

---

## 🏭 ETL Use Cases Summary

| Decorator                   | ETL Use Case               | Benefit                          |
| --------------------------- | -------------------------- | -------------------------------- |
| `@timer`                    | Measure each pipeline step | Identify performance bottlenecks |
| `@log_etl_step`             | Log start/end of each step | Creates an audit trail           |
| `@retry(max_attempts=3)`    | DB/API transient failures  | Makes pipeline resilient         |
| `@validate_dataframe(cols)` | Guard transform inputs     | Clear errors at entry point      |
| `@lru_cache`                | Cache lookup table queries | Avoid repeated DB calls          |

---

## 🧠 Must-Remember Points

| Concept                  | What to Remember                                                   |
| ------------------------ | ------------------------------------------------------------------ |
| `@decorator` syntax      | Shorthand for `func = decorator(func)`                             |
| `@functools.wraps`       | **Always use this** in your wrappers — preserves function identity |
| `*args, **kwargs`        | Use in wrapper to accept ANY function signature                    |
| Decorator with arguments | Requires 3 levels: factory → decorator → wrapper                   |
| Stacking decorators      | Applied bottom-up; outermost runs first                            |
| `@lru_cache`             | Built-in memoization — cache function results by arguments         |

---

## ⚠️ Common Mistakes Beginners Make

```python
import functools

# ❌ MISTAKE 1: Forgetting @functools.wraps
def bad_timer(func):
    def wrapper(*args, **kwargs):   # No @functools.wraps!
        return func(*args, **kwargs)
    return wrapper

@bad_timer
def my_etl_step():
    pass

print(my_etl_step.__name__)   # 'wrapper' — lost the original name!
# This breaks debugging, logging, and documentation tools.

# ✅ FIX: Always use @functools.wraps
def good_timer(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


# ❌ MISTAKE 2: Calling the function instead of passing it
@timer()     # This tries to call timer() with no arguments — TypeError!
def my_func():
    pass

# ✅ FIX: No parentheses needed for simple decorators
@timer
def my_func():
    pass


# ❌ MISTAKE 3: Forgetting to return the result from wrapper
def broken_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        func(*args, **kwargs)   # Result is discarded!
    return wrapper

@broken_decorator
def get_records():
    return [1, 2, 3]

result = get_records()
print(result)    # None — the [1, 2, 3] was thrown away!

# ✅ FIX: Always return the result
def correct_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)   # ← return is essential!
    return wrapper
```
