# 📋 Logging in Python (ETL Context)

---

## 📖 Explanation

The `logging` module provides a flexible logging system for tracking events, debugging, and monitoring. In ETL pipelines, proper logging is critical for **observability** — knowing what happened, when, and why the pipeline succeeded or failed.

---

## 🧠 Must-Remember Points

- Use `logging` module, **never** `print()` in production ETL.
- 5 log levels (in ascending severity): `DEBUG < INFO < WARNING < ERROR < CRITICAL`.
- `logging.basicConfig()` for quick setup; use `Handlers` for production-grade logging.
- `FileHandler` writes to file; `StreamHandler` writes to console.
- `RotatingFileHandler` prevents log files from growing indefinitely.
- Use `%(asctime)s`, `%(levelname)s`, `%(name)s`, `%(message)s` in format strings.
- Always use **named loggers** `logging.getLogger(__name__)` — never use root logger in modules.
- `logging.exception(msg)` automatically includes the full traceback.
- `logging.config.dictConfig()` for YAML/dict-based logging configuration.
- Log structured data (JSON) for log aggregation tools (Splunk, ELK, CloudWatch).

---

## 💻 Code Examples

### 1️⃣ Basic Logging Setup

```python
import logging

# Basic config (simple setup)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.debug("Debug: Checking value...")     # Detailed info for debugging
logging.info("Info: ETL started.")            # Confirmation it's working
logging.warning("Warning: Null values found.") # Something unexpected
logging.error("Error: DB connection failed!")  # A serious error
logging.critical("Critical: ETL cannot run!") # System cannot continue
```

---

### 2️⃣ Named Loggers (Best Practice for Modules)

```python
# etl/extractor.py
import logging

logger = logging.getLogger(__name__)  # Logger name = 'etl.extractor'

def extract_from_csv(filepath):
    logger.info(f"Starting extraction from: {filepath}")
    try:
        with open(filepath) as f:
            records = f.readlines()
        logger.info(f"Extracted {len(records)} records from {filepath}")
        return records
    except FileNotFoundError:
        logger.error(f"Source file not found: {filepath}")
        raise

# etl/main.py
import logging

# Configure once at the entry point
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

def run_pipeline():
    logger.info("Pipeline started.")
    data = extract_from_csv("sales.csv")
    logger.info("Pipeline complete.")
```

---

### 3️⃣ ETL: Production-Grade Logging (File + Console)

```python
import logging
from logging.handlers import RotatingFileHandler
import os

def setup_etl_logger(pipeline_name: str, log_dir: str = "logs") -> logging.Logger:
    """Set up a logger that writes to both file and console."""
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(pipeline_name)
    logger.setLevel(logging.DEBUG)

    # Formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler (INFO and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # File handler (DEBUG and above, rotating — max 10MB, keep 5 backups)
    file_handler = RotatingFileHandler(
        filename=os.path.join(log_dir, f"{pipeline_name}.log"),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

# Usage
logger = setup_etl_logger("sales_etl")
logger.info("ETL pipeline initialized.")
logger.debug("Config loaded: batch_size=1000, target='postgres'")
```

---

### 4️⃣ Logging Exceptions with Full Traceback

```python
import logging

logger = logging.getLogger(__name__)

def load_records(records, db_connection):
    try:
        # Simulate a DB insert
        if not records:
            raise ValueError("Cannot load empty record set.")
        # db_connection.executemany("INSERT INTO ...", records)
        logger.info(f"Loaded {len(records)} records successfully.")
    except ValueError as e:
        logger.exception("Load failed due to invalid data.")  # Includes full traceback!
        raise
    except Exception as e:
        logger.exception(f"Unexpected error during load: {e}")
        raise

try:
    load_records([], None)
except ValueError:
    pass  # Handled above
```

---

### 5️⃣ Structured JSON Logging (For ELK/Splunk/CloudWatch)

```python
import logging
import json
from datetime import datetime

class JsonFormatter(logging.Formatter):
    """Formats log records as JSON for log aggregation tools."""
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)

# Setup
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())

logger = logging.getLogger("etl.structured")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

logger.info("ETL step started", extra={"step": "extract"})
logger.error("Connection timeout")
# Output: {"timestamp":"2024-01-15T10:30:00.000Z","level":"INFO","logger":"etl.structured",...}
```

---

### 6️⃣ ETL Pipeline Logging Pattern

```python
import logging
import time
from contextlib import contextmanager

logger = logging.getLogger("etl.pipeline")

@contextmanager
def log_step(step_name: str):
    """Context manager that logs start, end, and duration of each ETL step."""
    logger.info(f"▶  Step started: {step_name}")
    start = time.perf_counter()
    try:
        yield
        elapsed = time.perf_counter() - start
        logger.info(f"✅ Step completed: {step_name} in {elapsed:.2f}s")
    except Exception as e:
        elapsed = time.perf_counter() - start
        logger.error(f"❌ Step FAILED: {step_name} after {elapsed:.2f}s — {e}")
        raise

# Usage
with log_step("Extract from S3"):
    time.sleep(0.3)  # Simulated
    data = [{"id": 1}]

with log_step("Transform — Clean nulls"):
    time.sleep(0.1)

with log_step("Load to PostgreSQL"):
    time.sleep(0.2)
```

---

## 🏭 ETL Use Cases

| Logging Pattern            | ETL Use Case                                   |
| -------------------------- | ---------------------------------------------- |
| Named loggers              | Per-module logs: `etl.extractor`, `etl.loader` |
| RotatingFileHandler        | Prevent log files from filling disk            |
| `logger.exception()`       | Log full tracebacks for errors                 |
| JSON logging               | Integration with ELK/CloudWatch/Splunk         |
| `log_step` context manager | Audit trail with timing for each ETL step      |
| `DEBUG` level              | Log SQL queries, row counts for debugging      |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Using print() in production ETL
def extract():
    print("Extracting...")  # No timestamp, no level, not filterable!

# RIGHT: Use logging
def extract():
    logger.info("Extracting data from source.")

# WRONG: Using the root logger in modules
logging.info("This is from root logger")  # Pollutes global logging namespace

# RIGHT: Use named loggers in every module
logger = logging.getLogger(__name__)  # e.g., 'etl.extractors.csv_reader'
logger.info("Extracting...")

# WRONG: Log inside tight loops (performance killer)
for record in million_records:
    logger.debug(f"Processing record: {record}")  # 1M log writes!

# RIGHT: Log summaries or sample
logger.info(f"Processing {len(million_records)} records...")
for i, record in enumerate(million_records):
    if i % 10000 == 0:
        logger.debug(f"Progress: {i}/{len(million_records)}")
```
