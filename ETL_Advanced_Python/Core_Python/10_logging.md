# 📋 Logging in Python (ETL Context)

---

## 🤔 What Is Logging and Why Should You Care?

ETL pipelines often run overnight, unattended. When something goes wrong at 3am, you need to know:

- **What happened?** — Which step failed?
- **When did it happen?** — Timestamp?
- **Why did it fail?** — What was the error message?

`print()` statements disappear when your script ends. **Logging** saves messages to files, formats them with timestamps, and lets you filter by severity.

> 💡 **Rule**: Never use `print()` in production ETL code. Always use `logging`.

---

## 🧱 The 5 Log Levels (Low → High Severity)

```python
import logging

logging.debug("Detailed info — only for debugging locally")      # Level 10
logging.info("Confirmation it's working — pipeline started")    # Level 20
logging.warning("Something unexpected but not critical")         # Level 30
logging.error("A real error — step failed, needs attention")     # Level 40
logging.critical("Catastrophic — pipeline cannot continue")      # Level 50
```

> 🔑 You set a **minimum level** — only messages at or above that level are shown. Setting `INFO` hides `DEBUG` messages; setting `WARNING` hides both `DEBUG` and `INFO`.

---

## 💻 Example 1: Quick Setup with `basicConfig`

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# %(asctime)s   = timestamp
# %(levelname)s = DEBUG/INFO/WARNING/ERROR/CRITICAL
# %(name)s      = which logger (useful when you have many modules)
# %(message)s   = your actual message

logging.info("ETL pipeline started.")
logging.debug("Config: batch_size=1000, target='postgres'")
logging.warning("Source file has 0 rows — nothing to process!")
logging.error("Connection to database failed.")
```

---

## 💻 Example 2: Named Loggers — The Right Way

Always create loggers per module using `__name__`. This lets you control logging per component:

```python
# In etl/extractor.py
import logging

logger = logging.getLogger(__name__)   # logger name = 'etl.extractor'

def extract_from_csv(filepath):
    logger.info(f"Starting extraction: {filepath}")
    try:
        with open(filepath) as f:
            rows = f.readlines()
        logger.info(f"Extracted {len(rows)} rows")
        return rows
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise

# In main.py — configure logging once at the entry point
import logging
logging.basicConfig(level=logging.INFO)

# All loggers (including 'etl.extractor') inherit this config
```

---

## 💻 Example 3: Production Setup — File + Console

```python
import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    """
    Creates a logger that writes to BOTH:
    - Console (INFO and above) — for live monitoring
    - File (DEBUG and above)   — for detailed audit trail

    RotatingFileHandler limits file size and keeps N backups.
    Without rotation, log files grow until the disk is full!
    """
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)   # Capture everything; handlers filter

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler — INFO and above only
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)

    # File handler — rotate at 10MB, keep 5 backups
    fh = RotatingFileHandler(
        f"{log_dir}/{name}.log",
        maxBytes=10 * 1024 * 1024,   # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(console)
    logger.addHandler(fh)
    return logger


# Usage in your ETL pipeline
logger = setup_logger("sales_etl")
logger.info("Pipeline started")
logger.debug("Processing batch 1 of 20")
logger.error("DB connection dropped")
```

---

## 💻 Example 4: Log Exceptions with Full Traceback

```python
import logging

logger = logging.getLogger(__name__)

def risky_transform(record):
    """This might fail for some records."""
    return int(record["qty"]) / int(record["price"])

records = [{"qty": "10", "price": "5"}, {"qty": "8", "price": "0"}]

for record in records:
    try:
        result = risky_transform(record)
        logger.info(f"Result: {result}")
    except Exception as e:
        # logger.exception() logs the message AND the full traceback automatically
        logger.exception(f"Transform failed for record: {record}")
        # Output includes:
        # ERROR | Transform failed for record: {'qty': '8', 'price': '0'}
        # Traceback (most recent call last):
        #   ...
        # ZeroDivisionError: division by zero
```

---

## 💻 Example 5: ETL Step Logging Context Manager

```python
import logging
import time
from contextlib import contextmanager

logger = logging.getLogger("etl.pipeline")

@contextmanager
def log_step(step_name: str):
    """Automatically logs start, end, and duration of each ETL step."""
    logger.info(f"▶  STARTED: {step_name}")
    start = time.perf_counter()
    try:
        yield
        elapsed = time.perf_counter() - start
        logger.info(f"✅ DONE: {step_name} in {elapsed:.2f}s")
    except Exception as e:
        elapsed = time.perf_counter() - start
        logger.error(f"❌ FAILED: {step_name} after {elapsed:.2f}s — {e}")
        raise

# Usage
with log_step("Extract from S3"):
    time.sleep(0.3)

with log_step("Transform — remove duplicates"):
    time.sleep(0.1)

with log_step("Load to PostgreSQL"):
    time.sleep(0.5)
```

---

## 🏭 ETL Use Cases Summary

| Pattern                      | Use Case                                 |
| ---------------------------- | ---------------------------------------- |
| `logger.info()/debug()`      | Step start, row counts, config values    |
| `logger.warning()`           | Empty batches, null fields, skipped rows |
| `logger.error()`             | Connection failures, transform errors    |
| `logger.exception()`         | Log error + FULL traceback               |
| `RotatingFileHandler`        | Prevent log files from filling disk      |
| `log_step()` context manager | Audit trail with timing per step         |

---

## 🧠 Must-Remember Points

| Rule                          | Remember                                               |
| ----------------------------- | ------------------------------------------------------ |
| Never `print()` in production | Use `logging` — structured, filterable, saved to file  |
| Named loggers                 | `logging.getLogger(__name__)` — one per module         |
| Configure once                | Call `basicConfig()` only in `main.py`, not in modules |
| `logger.exception()`          | Use inside `except` — includes automatic traceback     |
| `RotatingFileHandler`         | Prevents disk overflow from large log files            |

---

## ⚠️ Common Mistakes

```python
# ❌ Using root logger in modules
import logging
logging.info("...")   # Pollutes global namespace — use named loggers

# ✅ Named logger
logger = logging.getLogger(__name__)
logger.info("...")

# ❌ Logging inside tight loops (kills performance!)
for record in million_records:
    logger.debug(f"Processing: {record}")   # 1M log writes!

# ✅ Log summaries/milestones instead
for i, record in enumerate(million_records):
    if i % 10000 == 0:
        logger.info(f"Progress: {i:,}/{len(million_records):,}")
```
