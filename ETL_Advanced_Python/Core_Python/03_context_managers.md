# 🔐 Context Managers in Python (ETL Context)

---

## 🤔 What Is a Context Manager and Why Should You Care?

When your program works with **external resources** — files, database connections, locks, network sockets — something important must always happen:

> **Resources must be released when you're done with them.**

If you forget to close a file, you might corrupt it.  
If you forget to close a database connection, you can exhaust the connection pool and crash the entire application.  
If an exception occurs midway, your cleanup code in a `finally` block might be skipped…

**Context managers** solve all of this with a simple guarantee:

> **No matter what happens — success, error, or crash — the cleanup code always runs.**

> 💡 **Real-world analogy**: Think of a context manager like a **hotel room**. When you check IN, the hotel prepares the room for you (`__enter__`). When you check OUT, they clean it up — regardless of how messy you left it (`__exit__`). You don't need to worry about cleaning; the hotel handles it.

---

## 🧱 The `with` Statement — How It Works

You've already seen context managers if you've ever opened a file in Python:

```python
# WITHOUT a context manager (risky!)
f = open("data.csv", "r")
data = f.read()
# ⚠️ If an error happens here, f.close() never runs!
# The file stays open, consuming resources.
f.close()   # You have to remember this every time

# WITH a context manager (safe!)
with open("data.csv", "r") as f:
    data = f.read()
    # ⚠️ Even if an error happens here, 'with' ensures f.close() is called!
# File is guaranteed to be closed at this point — always.
```

### What Happens Behind the Scenes

When you write `with X as Y:`, Python:

1. Calls `X.__enter__()` — sets up the resource, returns it (assigned to `Y`)
2. Runs your code inside the `with` block
3. Calls `X.__exit__()` — cleans up, **no matter what** (success or exception)

---

## 🧱 Building Your Own Context Manager (Two Ways)

### Way 1: Class with `__enter__` and `__exit__`

```python
class DatabaseConnection:
    """
    A context manager that handles database connections.

    Usage:
        with DatabaseConnection("localhost", "etl_db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ...")
    """

    def __init__(self, host, dbname):
        self.host = host
        self.dbname = dbname
        self.connection = None   # Will hold the connection object

    def __enter__(self):
        """
        Called when entering the 'with' block.
        Sets up the resource and returns it.
        The returned value is assigned to the 'as' variable.
        """
        print(f"🔌 Opening connection to {self.dbname} at {self.host}...")
        # In real code: self.connection = psycopg2.connect(host=self.host, dbname=self.dbname)
        self.connection = {"host": self.host, "db": self.dbname, "status": "open"}
        return self.connection     # This is what 'conn' gets in the 'with ... as conn' line

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Called when leaving the 'with' block — always, no matter what.

        Parameters explain WHY we're leaving:
        - exc_type: The type of exception (None if no exception occurred)
        - exc_val:  The exception message/object (None if no exception)
        - exc_tb:   The traceback object (None if no exception)

        Return value:
        - Return False (or None) to let exceptions propagate normally
        - Return True to suppress the exception (use very rarely!)
        """
        if exc_type is not None:
            # An exception occurred inside the 'with' block
            print(f"💥 Error occurred: {exc_val}")
            print("🔄 Rolling back any uncommitted changes...")
            # In real code: self.connection.rollback()
        else:
            # No exception — everything went fine
            print("✅ Committing transaction...")
            # In real code: self.connection.commit()

        # Always close the connection
        print(f"🔒 Closing connection to {self.dbname}.")
        self.connection["status"] = "closed"
        # In real code: self.connection.close()

        return False   # Don't suppress exceptions — let them propagate


# ---- Using the context manager ----
# Happy path (no errors):
with DatabaseConnection("localhost", "etl_db") as conn:
    print(f"Using connection: {conn}")
    # cursor.execute("INSERT INTO ...")
# Output:
# 🔌 Opening connection to etl_db at localhost...
# Using connection: {'host': 'localhost', 'db': 'etl_db', 'status': 'open'}
# ✅ Committing transaction...
# 🔒 Closing connection to etl_db.

# Error path (exception inside with block):
try:
    with DatabaseConnection("localhost", "etl_db") as conn:
        raise RuntimeError("Disk full! Cannot write data.")
except RuntimeError:
    print("Pipeline failed, but connection was cleaned up safely!")
# Output:
# 🔌 Opening connection...
# 💥 Error occurred: Disk full! Cannot write data.
# 🔄 Rolling back...
# 🔒 Closing connection to etl_db.
# Pipeline failed, but connection was cleaned up safely!
```

---

### Way 2: Using `contextlib.contextmanager` (Simpler — Recommended!)

Instead of writing a whole class, you can use a **generator function** with `yield`:

```python
from contextlib import contextmanager
import time

@contextmanager
def etl_timer(step_name: str):
    """
    Context manager that times an ETL step.

    HOW IT WORKS:
    - Code BEFORE yield = __enter__ (setup)
    - The yield = the point where the 'with' block runs
    - Code AFTER yield (in finally) = __exit__ (cleanup)

    Usage:
        with etl_timer("Extract customers"):
            data = load_customers()
    """
    print(f"\n▶  [{step_name}] Starting...")
    start_time = time.perf_counter()

    try:
        yield       # Execution pauses here — the 'with' block runs now
        # If no exception: code continues here after the 'with' block ends
        elapsed = time.perf_counter() - start_time
        print(f"✅ [{step_name}] Completed in {elapsed:.3f}s")

    except Exception as e:
        # If an exception occurred in the 'with' block
        elapsed = time.perf_counter() - start_time
        print(f"❌ [{step_name}] FAILED after {elapsed:.3f}s — {e}")
        raise       # Re-raise the exception so it's not swallowed


# Usage in an ETL pipeline
with etl_timer("Extract from S3"):
    import time
    time.sleep(0.3)                # Simulate file download
    data = [{"id": 1}, {"id": 2}]

with etl_timer("Transform — Remove Duplicates"):
    time.sleep(0.1)               # Simulate processing

with etl_timer("Load to PostgreSQL"):
    time.sleep(0.5)               # Simulate DB writes

# ▶  [Extract from S3] Starting...
# ✅ [Extract from S3] Completed in 0.301s
# ▶  [Transform — Remove Duplicates] Starting...
# ✅ [Transform — Remove Duplicates] Completed in 0.101s
# ▶  [Load to PostgreSQL] Starting...
# ✅ [Load to PostgreSQL] Completed in 0.501s
```

---

## 💻 Practical ETL Examples

### Example 1: Database Transaction (Commit or Rollback)

```python
from contextlib import contextmanager

@contextmanager
def db_transaction(connection):
    """
    Context manager that wraps database operations in a transaction.

    WHY TRANSACTIONS?
    In ETL, you often insert data in multiple steps (e.g., insert a customer,
    then insert their orders). If step 2 fails, you want to ROLLBACK step 1 too
    — otherwise your database has partial/inconsistent data.

    Usage:
        with db_transaction(conn) as cursor:
            cursor.execute("INSERT INTO customers VALUES (%s)", (customer_id,))
            cursor.execute("INSERT INTO orders VALUES (%s, %s)", (order_id, customer_id))
        # If both succeed → COMMIT (data saved permanently)
        # If either fails → ROLLBACK (nothing is saved — all or nothing!)
    """
    cursor = connection.cursor()
    print("  🏦 Transaction started")
    try:
        yield cursor          # Give the cursor to the 'with' block
        connection.commit()   # All commands succeeded → save to database
        print("  ✅ Transaction COMMITTED — data saved permanently")

    except Exception as e:
        connection.rollback() # Something failed → undo everything
        print(f"  🔄 Transaction ROLLED BACK — no data written ({e})")
        raise                 # Propagate the error

    finally:
        cursor.close()        # Always close the cursor
        print("  🔒 Cursor closed")
```

---

### Example 2: Temporary Workspace (Create & Auto-Delete)

```python
import os
import shutil
import tempfile
from contextlib import contextmanager

@contextmanager
def temp_etl_workspace(prefix="etl_"):
    """
    Creates a temporary directory for intermediate ETL files.
    Automatically deletes it (and all contents) when done.

    WHY THIS IS USEFUL:
    ETL pipelines often need temporary space for:
    - Staging files before loading to a database
    - Unzipping compressed source files
    - Intermediate CSV/JSON files between pipeline steps

    Without cleanup, these temp files accumulate and fill your disk!
    This context manager guarantees cleanup even if the pipeline crashes.
    """
    # Create a unique temporary directory
    tmpdir = tempfile.mkdtemp(prefix=prefix)
    print(f"📁 Created temp workspace: {tmpdir}")

    try:
        yield tmpdir         # Give the path to the 'with' block
    finally:
        # This block ALWAYS runs — clean up temp files
        shutil.rmtree(tmpdir, ignore_errors=True)
        print(f"🗑️  Temp workspace deleted: {tmpdir}")


# Using the temp workspace
with temp_etl_workspace(prefix="sales_etl_") as workspace:
    # Create intermediate files
    staging_path = os.path.join(workspace, "staged_sales.csv")
    with open(staging_path, "w") as f:
        f.write("id,amount\n1,500\n2,300\n")
    print(f"📄 Staged file: {staging_path}")

    # Process the staged file...
    print("⚙️  Processing staged data...")

# When we exit the 'with' block, the entire temp directory is deleted!
# os.path.exists(staging_path) → False
```

---

### Example 3: `contextlib.suppress` — Ignore Specific Errors

```python
from contextlib import suppress
import os

# WITHOUT suppress — verbose and noisy
try:
    os.remove("/tmp/etl_lock_file.tmp")
except FileNotFoundError:
    pass    # We don't care if it didn't exist

# WITH suppress — expressive, one line
with suppress(FileNotFoundError):
    os.remove("/tmp/etl_lock_file.tmp")
print("✅ Cleanup done (file may or may not have existed)")

# You can suppress multiple exception types
with suppress(FileNotFoundError, PermissionError):
    os.remove("/tmp/etl_staging.csv")
```

---

### Example 4: `contextlib.ExitStack` — Dynamic Number of Context Managers

```python
from contextlib import ExitStack

# Problem: What if you need to open a variable number of files?
# You can't write 'with open(f1) as a, open(f2) as b, open(f3) as c:'
# if you don't know how many files there are at write time!

monthly_files = [
    "jan_sales.csv",
    "feb_sales.csv",
    "mar_sales.csv",
    # Could be 1 or could be 100 — we don't know at write time
]

# ExitStack manages multiple context managers dynamically
with ExitStack() as stack:
    # Open all files — each is registered with the stack
    file_handles = [
        stack.enter_context(open(filename, "w"))   # 'with open(filename)' dynamically
        for filename in monthly_files
    ]

    # Write to each file
    for i, fh in enumerate(file_handles):
        fh.write(f"data for month {i + 1}\n")
        print(f"  Wrote to {monthly_files[i]}")

# All files are closed automatically when ExitStack exits!
print("✅ All files closed.")
```

---

## 🏭 ETL Use Cases Summary

| Context Manager                         | ETL Use Case                         | Guarantee                     |
| --------------------------------------- | ------------------------------------ | ----------------------------- |
| `with open(...)`                        | Read/write CSV, JSON, log files      | File always closed            |
| `DatabaseConnection` / `db_transaction` | Atomic DB read/write                 | Commit or rollback            |
| `etl_timer()`                           | Benchmark each step                  | Logs duration even on failure |
| `temp_etl_workspace()`                  | Staging/intermediate files           | Temp dir always deleted       |
| `suppress()`                            | Non-critical cleanup steps           | Silently handles known errors |
| `ExitStack`                             | Variable number of files/connections | All closed on exit            |

---

## 🧠 Must-Remember Points

| Concept                     | What to Remember                                            |
| --------------------------- | ----------------------------------------------------------- |
| `with` statement            | Calls `__enter__` on entry, `__exit__` on exit — **always** |
| `__exit__` parameters       | `(exc_type, exc_val, exc_tb)` — all `None` if no exception  |
| Return `True` in `__exit__` | Suppresses the exception — use very rarely                  |
| `@contextmanager`           | Simpler way to create a context manager using `yield`       |
| Code before `yield`         | Runs as `__enter__` (setup)                                 |
| Code after `yield`          | Runs as `__exit__` (cleanup) — put in `finally` for safety  |
| `suppress()`                | Cleaner way to ignore specific exceptions                   |
| `ExitStack`                 | For dynamic/variable numbers of context managers            |

---

## ⚠️ Common Mistakes Beginners Make

```python
# ❌ MISTAKE 1: Re-using a closed context manager
with open("file.csv") as f:
    data = f.read()
# 'f' is now closed!
more_data = f.read()    # ValueError: I/O operation on closed file

# ✅ FIX: Keep all file operations inside the 'with' block
with open("file.csv") as f:
    data = f.read()
    more_data = f.read()   # Fine — still inside 'with' block


# ❌ MISTAKE 2: Suppressing ALL exceptions in __exit__ (dangerous!)
def __exit__(self, exc_type, exc_val, exc_tb):
    self.cleanup()
    return True   # Suppresses EVERYTHING — even real bugs are hidden!

# ✅ FIX: Only suppress exceptions you intend to handle
def __exit__(self, exc_type, exc_val, exc_tb):
    self.cleanup()
    return False  # Let all exceptions propagate (this is usually what you want)


# ❌ MISTAKE 3: In @contextmanager, forgetting 'try/finally' for guaranteed cleanup
from contextlib import contextmanager

@contextmanager
def bad_resource_manager():
    resource = acquire_resource()   # Set up
    yield resource
    release_resource(resource)     # ← NOT in finally! If error occurs, this is skipped!

@contextmanager
def good_resource_manager():
    resource = acquire_resource()   # Set up
    try:
        yield resource
    finally:
        release_resource(resource)  # ✅ Always runs, even if an error occurs
```
