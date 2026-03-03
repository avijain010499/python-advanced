# 🔐 Context Managers in Python (ETL Context)

---

## 📖 Explanation

A **context manager** manages resources (files, database connections, locks) using the `with` statement. It guarantees that setup and teardown code runs correctly — even if an exception occurs.

Context managers implement `__enter__()` and `__exit__()` methods, or can be created using `contextlib.contextmanager` decorator.

---

## 🧠 Must-Remember Points

- `with` statement ensures `__exit__` always runs (like `finally`).
- `__enter__` returns the resource; `__exit__` handles cleanup.
- `contextlib.contextmanager` lets you create one using `yield` (simpler than a class).
- `contextlib.suppress(Exception)` suppresses specified exceptions.
- `contextlib.ExitStack` manages multiple context managers dynamically.
- Always use context managers for: file I/O, DB connections, transactions, locks.
- The `as` clause captures the value returned by `__enter__`.
- `__exit__(self, exc_type, exc_val, exc_tb)` — return `True` to suppress an exception.

---

## 💻 Code Examples

### 1️⃣ The Classic: File I/O with `with`

```python
# WITHOUT context manager (risky — file may stay open on error)
f = open("data.csv", "r")
data = f.read()
f.close()  # This might not execute if an exception occurs!

# WITH context manager (safe — always closes)
with open("data.csv", "r") as f:
    data = f.read()
# File is guaranteed to be closed here
```

---

### 2️⃣ Custom Class-Based Context Manager

```python
class DatabaseConnection:
    def __init__(self, host, dbname):
        self.host = host
        self.dbname = dbname
        self.connection = None

    def __enter__(self):
        print(f"Connecting to {self.dbname} at {self.host}...")
        # self.connection = psycopg2.connect(host=self.host, dbname=self.dbname)
        self.connection = {"host": self.host, "db": self.dbname}  # Simulated
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print(f"Exception occurred: {exc_val}. Rolling back...")
            # self.connection.rollback()
        else:
            print("Committing transaction...")
            # self.connection.commit()
        print("Closing connection.")
        # self.connection.close()
        return False  # Don't suppress the exception

with DatabaseConnection("localhost", "etl_db") as conn:
    print(f"Running ETL on: {conn}")
    # cursor.execute("INSERT INTO ...")
```

---

### 3️⃣ `contextlib.contextmanager` — Generator-based Context Manager

```python
from contextlib import contextmanager
import time

@contextmanager
def etl_timer(step_name: str):
    """Context manager to time an ETL step."""
    print(f"[START] {step_name}")
    start = time.perf_counter()
    try:
        yield  # Code inside 'with' block runs here
    except Exception as e:
        print(f"[ERROR] {step_name} failed: {e}")
        raise
    finally:
        elapsed = time.perf_counter() - start
        print(f"[END] {step_name} completed in {elapsed:.4f}s")

# Usage
with etl_timer("Extract from API"):
    import time
    time.sleep(0.3)  # Simulated API call
    data = [{"id": 1}, {"id": 2}]

with etl_timer("Transform Data"):
    result = [row for row in data if row["id"] > 0]
```

---

### 4️⃣ ETL: Database Transaction Context Manager

```python
from contextlib import contextmanager

@contextmanager
def db_transaction(connection):
    """Ensures ETL DB writes are atomic — commit or rollback."""
    cursor = connection.cursor()
    try:
        yield cursor
        connection.commit()
        print("Transaction committed successfully.")
    except Exception as e:
        connection.rollback()
        print(f"Transaction rolled back due to: {e}")
        raise
    finally:
        cursor.close()

# Usage with a real psycopg2 connection:
# import psycopg2
# conn = psycopg2.connect("dbname=etl_db user=postgres")
# with db_transaction(conn) as cursor:
#     cursor.execute("INSERT INTO sales VALUES (%s, %s)", (1, 500))
#     cursor.execute("INSERT INTO orders VALUES (%s)", (101,))
```

---

### 5️⃣ ETL: Temporary Directory Context Manager

```python
import tempfile
import os
from contextlib import contextmanager

@contextmanager
def temp_workspace():
    """Creates a temp directory for ETL intermediate files, cleans up after."""
    import tempfile, shutil
    tmpdir = tempfile.mkdtemp(prefix="etl_workspace_")
    print(f"Created temp workspace: {tmpdir}")
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir)
        print(f"Cleaned up temp workspace: {tmpdir}")

with temp_workspace() as workspace:
    # Write intermediate files during ETL
    staging_file = os.path.join(workspace, "staged_data.csv")
    with open(staging_file, "w") as f:
        f.write("id,value\n1,100\n2,200\n")
    print(f"Staged file: {staging_file}")
# Temp directory is deleted automatically
```

---

### 6️⃣ `contextlib.suppress` — Silently Skip Errors

```python
from contextlib import suppress
import os

# Silently ignore FileNotFoundError when deleting temp files
with suppress(FileNotFoundError):
    os.remove("nonexistent_temp_file.csv")
print("Continuing ETL regardless...")
```

---

### 7️⃣ `contextlib.ExitStack` — Dynamic Context Managers

```python
from contextlib import ExitStack

files = ["file1.csv", "file2.csv", "file3.csv"]

# Open multiple files dynamically (number not known at write time)
with ExitStack() as stack:
    handles = [stack.enter_context(open(f, "w")) for f in files]
    for i, fh in enumerate(handles):
        fh.write(f"data for file {i}\n")
# All files are safely closed here
```

---

## 🏭 ETL Use Cases

| Context Manager           | ETL Use Case                              |
| ------------------------- | ----------------------------------------- |
| `open()`                  | Safe file reading/writing for CSVs, JSONs |
| Custom DB context manager | Atomic transactions (commit/rollback)     |
| `etl_timer()`             | Benchmark each pipeline step              |
| `temp_workspace()`        | Manage staging/temp directories           |
| `suppress()`              | Skip non-critical cleanup errors          |
| `ExitStack`               | Open N files or connections dynamically   |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Re-entering a closed context manager
with open("file.csv") as f:
    data = f.read()
# file is closed now!
data2 = f.read()  # ValueError: I/O operation on closed file

# WRONG: Swallowing exceptions by returning True in __exit__
def __exit__(self, exc_type, exc_val, exc_tb):
    return True  # This hides ALL exceptions — dangerous in ETL!

# RIGHT: Only suppress known, safe exceptions
def __exit__(self, exc_type, exc_val, exc_tb):
    if exc_type is KeyboardInterrupt:
        return False  # Don't suppress
    self.cleanup()
    return False  # Let other exceptions propagate
```
