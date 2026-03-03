# 🛡️ Exception Handling in Python (ETL Context)

---

## 📖 Explanation

Exception handling lets your code **gracefully respond to errors** instead of crashing. In ETL, robust error handling is critical — a single bad record or connection failure should not abort an entire pipeline run.

Python's exception handling uses `try`, `except`, `else`, `finally`, and `raise`.

---

## 🧠 Must-Remember Points

- `try`: Code that might raise an exception.
- `except ExceptionType as e`: Catch specific exceptions.
- `else`: Runs only if NO exception occurred in `try`.
- `finally`: ALWAYS runs — use for cleanup (close files, connections).
- Catch the **most specific** exception type first (bottom of hierarchy last).
- Never use bare `except:` — always specify the exception type.
- `raise` re-raises the current exception; `raise MyError(...)` raises a new one.
- Custom exceptions should inherit from `Exception` (not `BaseException`).
- Use `logging` instead of `print` for exception messages in production ETL.
- `ExceptionGroup` (Python 3.11+) handles multiple exceptions simultaneously.
- `traceback.format_exc()` gives the full traceback as a string.

---

## 💻 Code Examples

### 1️⃣ Basic Try-Except-Else-Finally

```python
def parse_record(record):
    try:
        value = int(record["value"])  # Might raise ValueError
        result = 100 / value           # Might raise ZeroDivisionError
    except ValueError as e:
        print(f"Invalid value format: {e}")
        return None
    except ZeroDivisionError:
        print("Value cannot be zero.")
        return None
    except (KeyError, TypeError) as e:
        print(f"Bad record structure: {e}")
        return None
    else:
        # Only runs if NO exception
        print(f"Parsed successfully: {result}")
        return result
    finally:
        # ALWAYS runs
        print("Record processing attempt complete.")

parse_record({"value": "10"})   # Success
parse_record({"value": "0"})    # ZeroDivisionError
parse_record({"value": "abc"})  # ValueError
```

---

### 2️⃣ ETL: Process Records with Error Quarantine

```python
import logging

logging.basicConfig(level=logging.INFO)

def process_batch(records):
    """Process a batch; quarantine bad records instead of failing the whole batch."""
    good_records = []
    bad_records = []

    for idx, record in enumerate(records):
        try:
            # Simulate transformation
            transformed = {
                "id": int(record["id"]),
                "amount": float(record["amount"]),
                "name": record["name"].strip().upper(),
            }
            good_records.append(transformed)
        except (KeyError, ValueError, AttributeError) as e:
            logging.warning(f"Record {idx} failed validation: {e} | Data: {record}")
            bad_records.append({"record": record, "error": str(e), "index": idx})

    logging.info(f"Processed: {len(good_records)} valid, {len(bad_records)} rejected")
    return good_records, bad_records

records = [
    {"id": "1", "amount": "500.00", "name": " alice "},
    {"id": "ABC", "amount": "200.00", "name": "bob"},  # Bad id
    {"id": "3", "amount": "not_a_number", "name": "charlie"},  # Bad amount
    {"id": "4", "amount": "300.00", "name": None},  # Bad name
]

good, bad = process_batch(records)
print(f"Good: {good}")
print(f"Bad: {bad}")
```

---

### 3️⃣ ETL: Retry Logic with Exception Handling

```python
import time
import logging

def connect_to_db(host, retries=3, delay=2):
    """Attempt DB connection with retry on failure."""
    for attempt in range(1, retries + 1):
        try:
            if attempt < 3:  # Simulate failures
                raise ConnectionError(f"Failed to connect to {host}")
            print(f"Connected to {host} on attempt {attempt}")
            return True  # Success
        except ConnectionError as e:
            logging.error(f"Attempt {attempt}: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logging.critical("All retry attempts exhausted.")
                raise  # Re-raise after all attempts
    return False

try:
    connect_to_db("postgres://localhost:5432/etl_db")
except ConnectionError:
    print("ETL pipeline cannot start — database unavailable.")
```

---

### 4️⃣ Custom Exception Classes for ETL

```python
class ETLError(Exception):
    """Base class for all ETL exceptions."""
    pass

class ExtractionError(ETLError):
    """Raised when data extraction fails."""
    def __init__(self, source, reason):
        self.source = source
        self.reason = reason
        super().__init__(f"Extraction from '{source}' failed: {reason}")

class TransformationError(ETLError):
    """Raised when a transformation step fails."""
    def __init__(self, step, record, reason):
        self.step = step
        self.record = record
        self.reason = reason
        super().__init__(f"Transform step '{step}' failed on record {record}: {reason}")

class LoadError(ETLError):
    """Raised when data loading fails."""
    pass

# Usage
def extract_from_api(url):
    import random
    if random.random() < 0.5:
        raise ExtractionError(url, "HTTP 503 Service Unavailable")
    return [{"id": 1, "value": 100}]

def run_etl_pipeline():
    url = "https://api.example.com/data"
    try:
        data = extract_from_api(url)
    except ExtractionError as e:
        print(f"ALERT: {e}")
        print(f"Source: {e.source}, Reason: {e.reason}")
        raise  # Propagate to orchestrator (e.g., Airflow)

run_etl_pipeline()
```

---

### 5️⃣ Using `finally` for Guaranteed Cleanup

```python
import csv

def load_to_csv(records, output_path):
    """Write records to CSV; always close the file."""
    f = None
    try:
        f = open(output_path, "w", newline="")
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
        print(f"Successfully loaded {len(records)} records to {output_path}")
    except (IOError, OSError) as e:
        print(f"File error: {e}")
        raise LoadError(f"Cannot write to {output_path}: {e}") from e
    except (IndexError, KeyError) as e:
        print(f"Data error: {e}")
        raise
    finally:
        if f and not f.closed:
            f.close()
            print("File handle closed (finally block).")
```

---

### 6️⃣ `traceback` Module — Full Error Info for Logging

```python
import traceback
import logging

logging.basicConfig(level=logging.ERROR)

def risky_transform(record):
    return int(record["amount"]) / int(record["qty"])

records = [{"amount": "100", "qty": "0"}, {"amount": "abc", "qty": "5"}]

for record in records:
    try:
        result = risky_transform(record)
    except Exception as e:
        # Log full traceback as a string (for log files/monitoring)
        tb = traceback.format_exc()
        logging.error(f"Transform failed for record {record}:\n{tb}")
```

---

### 7️⃣ `contextlib.suppress` — Intentionally Ignore Specific Errors

```python
from contextlib import suppress
import os

# Remove temp file if it exists, ignore if it doesn't
with suppress(FileNotFoundError):
    os.remove("/tmp/etl_staging_file.csv")

# Equivalent to:
try:
    os.remove("/tmp/etl_staging_file.csv")
except FileNotFoundError:
    pass
```

---

## 🏭 ETL Use Cases

| Pattern                    | ETL Use Case                                 |
| -------------------------- | -------------------------------------------- |
| Try-except with quarantine | Process large batches; isolate bad records   |
| Retry with backoff         | Database / API connection failures           |
| Custom ETL exceptions      | Meaningful, tier-specific error messages     |
| `finally`                  | Guarantee file/connection cleanup            |
| `traceback.format_exc()`   | Structured error logging to monitoring tools |
| `suppress()`               | Silent cleanup operations                    |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Bare except catches EVERYTHING including KeyboardInterrupt
try:
    process_data()
except:  # ← NEVER do this
    pass

# WRONG: Catching too broadly
try:
    value = int(record["amount"])
except Exception:  # Hides ALL errors — hard to debug
    value = 0

# RIGHT: Catch specifically
try:
    value = int(record["amount"])
except (ValueError, TypeError):
    value = 0
except KeyError:
    logging.warning("Missing 'amount' field")
    value = 0

# WRONG: Losing the original exception context
try:
    data = read_file("file.csv")
except IOError as e:
    raise ETLError("File read failed")  # Original traceback is lost!

# RIGHT: Chain exceptions with 'from'
try:
    data = read_file("file.csv")
except IOError as e:
    raise ETLError("File read failed") from e  # Preserves original traceback
```
