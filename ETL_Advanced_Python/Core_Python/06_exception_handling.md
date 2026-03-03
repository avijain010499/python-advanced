# 🛡️ Exception Handling in Python (ETL Context)

---

## 🤔 What Is Exception Handling and Why Should You Care?

When your code runs, unexpected things happen: a source file doesn't exist, a database connection times out, or a record has `"abc"` where a number was expected.

Without exception handling, any of these would **crash your entire ETL pipeline** — even if 999,999 out of 1,000,000 records were perfectly fine.

**Exception handling** lets you catch errors, respond gracefully (log it, skip it, retry), and keep processing the remaining valid data.

> 💡 **Analogy**: Exception handling is like a quality control line in a factory. When a defective product arrives, a worker doesn't shut down the entire factory — they pull that item aside, log it, and let the line keep running.

---

## 🧱 The 4 Keywords: try, except, else, finally

```python
try:
    risky_operation()   # Code that MIGHT fail

except ValueError as e:
    # Runs ONLY if ValueError was raised. 'e' is the error object.
    handle_value_error(e)

except (KeyError, TypeError) as e:
    # Catch MULTIPLE exception types with a tuple — handles either one
    handle_key_or_type_error(e)

else:
    # Runs ONLY if NO exception occurred in 'try'
    do_success_logic()

finally:
    # ALWAYS runs — with or without an exception
    # Use for cleanup: closing files, connections, locks
    cleanup()
```

---

## 🧱 Common Exception Types in ETL

| Exception           | When it Happens        | ETL Example                  |
| ------------------- | ---------------------- | ---------------------------- |
| `ValueError`        | Bad value for the type | `int("abc")` — can't convert |
| `KeyError`          | Dict key doesn't exist | `row["missing_col"]`         |
| `TypeError`         | Wrong type used        | `None.strip()`               |
| `FileNotFoundError` | File doesn't exist     | `open("missing.csv")`        |
| `ConnectionError`   | DB/API unreachable     | `psycopg2.connect(...)`      |
| `ZeroDivisionError` | Division by zero       | `revenue / 0`                |

---

## 💻 Example 1: Basic try/except

```python
# Safe type conversion (very common in ETL)
raw_values = ["100", "200", "bad_data", "400", None, "500"]

for raw in raw_values:
    try:
        # This might raise ValueError (if string isn't a number)
        # or TypeError (if raw is None — can't convert None to float)
        number = float(raw)
        print(f"✅ Converted: {number}")

    except ValueError:
        print(f"⚠️  Invalid number string: '{raw}' — skipping")

    except TypeError:
        print(f"⚠️  Got None instead of a number — skipping")

# Output:
# ✅ Converted: 100.0
# ✅ Converted: 200.0
# ⚠️  Invalid number string: 'bad_data' — skipping
# ✅ Converted: 400.0
# ⚠️  Got None instead of a number — skipping
# ✅ Converted: 500.0
```

---

## 💻 Example 2: ETL Quarantine Pattern ⭐ (Most Important)

Process all records. Bad ones go to a quarantine list instead of crashing the pipeline.

```python
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

def transform_record(raw):
    """Transform one raw record. Raises if data is broken."""
    return {
        "id":     int(raw["id"]),             # Raises ValueError if not numeric
        "name":   raw["name"].strip().title(), # Raises AttributeError if name is None
        "amount": float(raw.get("amount", 0)),
        "region": raw["region"].strip().upper(),
    }

def process_batch(records):
    good_records = []   # Successfully transformed records
    bad_records  = []   # Records that failed with error info

    for index, record in enumerate(records):
        try:
            transformed = transform_record(record)
            good_records.append(transformed)

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            # Catch all data-related errors
            error_msg = f"{type(e).__name__}: {e}"
            logging.warning(f"Row {index} failed — {error_msg} | Data: {record}")
            bad_records.append({"row": index, "record": record, "error": error_msg})

    logging.info(f"✅ Good: {len(good_records)} | ⚠️  Quarantined: {len(bad_records)}")
    return good_records, bad_records

raw_data = [
    {"id": "1",     "name": " Alice ", "amount": "500",   "region": "north"},
    {"id": "2",     "name": "Bob",     "amount": "invalid","region": "SOUTH"},
    {"id": "THREE", "name": "Charlie", "amount": "800",   "region": "east"},
    {"id": "4",     "name": None,      "amount": "300",   "region": "west"},
    {"id": "5",     "name": "Eve",     "amount": "1200",  "region": "north"},
]

good, bad = process_batch(raw_data)
print(f"Good: {good}")
print(f"Bad: {[b['error'] for b in bad]}")
```

---

## 💻 Example 3: Custom Exception Classes

Custom exceptions give you structure and clear error messages:

```python
class ETLError(Exception):
    """Base class for all ETL exceptions."""
    pass

class ExtractionError(ETLError):
    """Raised when data cannot be read from the source."""
    def __init__(self, source: str, reason: str):
        self.source = source
        self.reason = reason
        super().__init__(f"Extraction from '{source}' failed: {reason}")

class LoadError(ETLError):
    """Raised when data cannot be written to the target."""
    def __init__(self, target: str, reason: str):
        self.target = target
        self.reason = reason
        super().__init__(f"Load to '{target}' failed: {reason}")

# Usage
def extract_from_api(url: str) -> list:
    import random
    if random.random() < 0.5:
        raise ExtractionError(url, "HTTP 503 — server unavailable")
    return [{"id": 1, "value": 100}]

try:
    data = extract_from_api("https://api.example.com/sales")
    print(f"✅ Got {len(data)} records")
except ExtractionError as e:
    print(f"❌ {e}")
    print(f"   Source: {e.source}")    # Structured access to error details
    print(f"   Reason: {e.reason}")
    raise   # Re-raise so pipeline orchestrator knows the job failed
```

---

## 💻 Example 4: Retry Logic for Transient Failures

```python
import time

def connect_with_retry(host, dbname, max_attempts=3, wait_seconds=2):
    """Attempt DB connection with automatic retry on failure."""
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"  Attempt {attempt}/{max_attempts}...")
            if attempt < 3:  # Simulate failures for demo
                raise ConnectionError("Connection refused")
            print(f"  ✅ Connected on attempt {attempt}!")
            return {"host": host, "status": "open"}

        except ConnectionError as e:
            if attempt < max_attempts:
                print(f"  ⚠️  Failed: {e}. Retrying in {wait_seconds}s...")
                time.sleep(wait_seconds)
            else:
                raise ConnectionError(
                    f"All {max_attempts} attempts failed: {e}"
                )

try:
    conn = connect_with_retry("localhost", "etl_db")
except ConnectionError as e:
    print(f"❌ Cannot start pipeline: {e}")
```

---

## 💻 Example 5: `finally` for Guaranteed Cleanup

```python
import csv

def load_csv(filepath: str) -> list:
    """Load CSV file. File is ALWAYS closed, even on error."""
    f = None   # Define here so 'finally' can access it

    try:
        f = open(filepath, "r", encoding="utf-8")
        rows = list(csv.DictReader(f))

    except FileNotFoundError:
        print(f"❌ File not found: {filepath}")
        return []

    except csv.Error as e:
        print(f"❌ CSV parsing error: {e}")
        return []

    else:
        print(f"✅ Loaded {len(rows)} rows")
        return rows

    finally:
        # This runs whether or not an exception occurred
        if f and not f.closed:
            f.close()
            print("  📁 File handle closed safely")

data = load_csv("sales.csv")
```

---

## 💻 Example 6: Exception Chaining — Never Lose the Original Error

```python
# When catching one error and raising another, always chain them!
class ETLError(Exception):
    pass

def read_source_file(filepath):
    try:
        with open(filepath) as f:
            return f.read()
    except FileNotFoundError as e:
        # 'from e' preserves the original error as the cause
        raise ETLError(f"Source file not found: {filepath}") from e
        # Without 'from e', the FileNotFoundError traceback is hidden!

try:
    read_source_file("/data/missing.csv")
except ETLError as e:
    print(f"ETL Error: {e}")
    print(f"Caused by:  {e.__cause__}")  # Original FileNotFoundError
```

---

## 🏭 ETL Use Cases Summary

| Pattern            | When to Use                                                |
| ------------------ | ---------------------------------------------------------- |
| Basic `try/except` | Single record conversion (int, float, date parsing)        |
| Quarantine pattern | Batch processing — bad records aside, good ones continue   |
| Custom exceptions  | Structured, tier-specific error info with extra attributes |
| Retry loop         | DB/API connections that fail temporarily                   |
| `finally`          | Always close files, connections, cursors                   |
| `raise X from Y`   | Re-raise with full error chain preserved                   |

---

## 🧠 Must-Remember Points

| Rule                      | Remember                                        |
| ------------------------- | ----------------------------------------------- |
| Always name the exception | `except ValueError` not `except:`               |
| Catch specifically        | `ValueError, KeyError` — not broad `Exception`  |
| `else` clause             | "Only if no error" — rarely used but useful     |
| `finally`                 | "Always runs" — use for cleanup                 |
| `raise` alone             | Re-raises WITHOUT losing the original traceback |
| `raise X from Y`          | Chains exceptions — shows root cause            |

---

## ⚠️ Common Mistakes Beginners Make

```python
# ❌ MISTAKE 1: Bare except — catches EVERYTHING
try:
    process_data()
except:   # ← Catches KeyboardInterrupt, SystemExit, bugs! NEVER use this.
    pass

# ✅ FIX:
try:
    process_data()
except (ValueError, KeyError) as e:
    logging.warning(f"Skipping bad record: {e}")


# ❌ MISTAKE 2: Silently swallowing errors
try:
    conn = connect_to_db()
except ConnectionError:
    pass   # ← Error is hidden! Pipeline continues with no connection!

# ✅ FIX: Log AND re-raise
try:
    conn = connect_to_db()
except ConnectionError as e:
    logging.error(f"DB connection failed: {e}")
    raise


# ❌ MISTAKE 3: Forgetting 'from' when re-raising
try:
    data = read_file(path)
except IOError as e:
    raise ETLError("Read failed")   # Original traceback is lost!

# ✅ FIX: Chain with 'from'
try:
    data = read_file(path)
except IOError as e:
    raise ETLError("Read failed") from e  # Preserves full traceback
```
