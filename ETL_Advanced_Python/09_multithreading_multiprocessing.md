# ⚡ Multithreading & Multiprocessing in Python (ETL Context)

---

## 📖 Explanation

Python offers two main concurrency approaches:

- **Multithreading** (`threading`): Multiple threads share the same memory space. Due to the **GIL** (Global Interpreter Lock), only one thread runs Python bytecode at a time — best for **I/O-bound** tasks.
- **Multiprocessing** (`multiprocessing`): Multiple separate processes, each with its own memory. Bypasses the GIL — best for **CPU-bound** tasks.
- **`concurrent.futures`**: High-level API for both — use `ThreadPoolExecutor` and `ProcessPoolExecutor`.

---

## 🧠 Must-Remember Points

- **GIL**: Only one thread executes Python code at a time — multithreading doesn't speed up CPU-bound tasks.
- Use **ThreadPoolExecutor** for I/O-bound ETL (file reads, API calls, DB queries).
- Use **ProcessPoolExecutor** for CPU-bound ETL (heavy data transformations, ML inference).
- `executor.map()` applies a function in parallel and returns results in order.
- `executor.submit()` submits a single task and returns a `Future` object.
- `as_completed()` yields futures as they complete (order not guaranteed).
- Always use `with` statement with executors — ensures clean shutdown.
- Shared state is dangerous with threads — use `threading.Lock()` or `queue.Queue`.
- Processes cannot share Python objects directly — use `multiprocessing.Queue` or `Manager`.
- `max_workers` controls the thread/process pool size.

---

## 💻 Code Examples

### 1️⃣ Threading Basics

```python
import threading
import time

def download_file(url, file_id):
    print(f"[Thread {threading.current_thread().name}] Downloading file {file_id}...")
    time.sleep(1)  # Simulate I/O
    print(f"[Thread {threading.current_thread().name}] Done: file {file_id}")

# Sequential: ~5 seconds
# Threaded: ~1 second (I/O-bound, threads run concurrently during sleep)
threads = []
for i in range(5):
    t = threading.Thread(target=download_file, args=(f"http://example.com/{i}", i), name=f"T-{i}")
    threads.append(t)
    t.start()

for t in threads:
    t.join()  # Wait for all threads to finish
print("All downloads complete!")
```

---

### 2️⃣ ETL: `ThreadPoolExecutor` for Parallel API Extraction

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

def fetch_data_from_api(endpoint):
    """Simulate fetching data from an external API."""
    time.sleep(random.uniform(0.5, 2.0))  # Simulate network latency
    return {"endpoint": endpoint, "records": random.randint(100, 1000)}

endpoints = [
    "https://api.example.com/sales",
    "https://api.example.com/customers",
    "https://api.example.com/products",
    "https://api.example.com/inventory",
    "https://api.example.com/orders",
]

# Sequential would take ~5-10 seconds
# Parallel takes ~2 seconds (max of individual delays)
with ThreadPoolExecutor(max_workers=5) as executor:
    # Submit all tasks
    future_to_endpoint = {
        executor.submit(fetch_data_from_api, ep): ep
        for ep in endpoints
    }

    for future in as_completed(future_to_endpoint):
        endpoint = future_to_endpoint[future]
        try:
            result = future.result()
            print(f"✅ {endpoint}: {result['records']} records")
        except Exception as e:
            print(f"❌ {endpoint}: Failed — {e}")
```

---

### 3️⃣ ETL: `ProcessPoolExecutor` for CPU-Bound Transformations

```python
from concurrent.futures import ProcessPoolExecutor
import pandas as pd

def transform_partition(chunk_data):
    """Heavy CPU transformation — runs in separate process."""
    # Simulate heavy computation
    df = pd.DataFrame(chunk_data)
    df["revenue_normalized"] = df["revenue"] / df["revenue"].max()
    df["profit_margin"] = (df["revenue"] - df["cost"]) / df["revenue"] * 100
    return df.to_dict("records")

# Simulate large dataset split into chunks
raw_records = [{"revenue": i * 100, "cost": i * 60} for i in range(1, 10001)]
chunk_size = 2500
chunks = [
    raw_records[i:i + chunk_size]
    for i in range(0, len(raw_records), chunk_size)
]

if __name__ == "__main__":  # Required for multiprocessing on Windows/macOS
    with ProcessPoolExecutor(max_workers=4) as executor:
        # map() preserves order; processes chunks in parallel
        results = list(executor.map(transform_partition, chunks))

    # Flatten results
    all_records = [record for chunk in results for record in chunk]
    print(f"Transformed {len(all_records)} records using {len(chunks)} processes")
```

---

### 4️⃣ Thread-Safe Shared Data — `Lock` and `Queue`

```python
import threading
from queue import Queue
import time

# Thread-safe result collection using Queue
result_queue = Queue()

def extract_and_queue(source_id, q):
    """Extract records and put them into a shared queue."""
    time.sleep(0.5)
    records = [{"source": source_id, "value": i} for i in range(5)]
    q.put(records)
    print(f"  Source {source_id}: queued {len(records)} records")

# Producer threads (extract)
threads = []
for src_id in range(4):
    t = threading.Thread(target=extract_and_queue, args=(src_id, result_queue))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

# Collect all results
all_records = []
while not result_queue.empty():
    all_records.extend(result_queue.get())

print(f"Total records collected: {len(all_records)}")

# Using Lock for thread-safe counter
lock = threading.Lock()
counter = {"processed": 0}

def process_record(record):
    # Simulate processing
    time.sleep(0.01)
    with lock:  # Only one thread modifies counter at a time
        counter["processed"] += 1
```

---

### 5️⃣ ETL Pipeline with Thread-per-Layer Pattern

```python
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
import threading
import time

def extractor(extract_queue):
    """Produces data into the extract queue."""
    for i in range(20):
        extract_queue.put({"id": i, "raw_value": str(i * 10)})
        time.sleep(0.05)
    extract_queue.put(None)  # Sentinel value to signal completion
    print("[Extractor] Done.")

def transformer(extract_queue, load_queue):
    """Reads from extract queue, transforms, puts into load queue."""
    while True:
        try:
            record = extract_queue.get(timeout=2)
            if record is None:
                break
            transformed = {
                "id": record["id"],
                "value": int(record["raw_value"]) * 2
            }
            load_queue.put(transformed)
        except Empty:
            break
    load_queue.put(None)  # Signal loader
    print("[Transformer] Done.")

def loader(load_queue):
    """Reads transformed records and 'loads' them."""
    loaded = []
    while True:
        try:
            record = load_queue.get(timeout=2)
            if record is None:
                break
            loaded.append(record)
        except Empty:
            break
    print(f"[Loader] Loaded {len(loaded)} records.")
    return loaded

# Run pipeline stages concurrently
extract_q = Queue(maxsize=10)
load_q = Queue(maxsize=10)

with ThreadPoolExecutor(max_workers=3) as pool:
    pool.submit(extractor, extract_q)
    pool.submit(transformer, extract_q, load_q)
    future = pool.submit(loader, load_q)
```

---

## 🏭 ETL Use Cases

| Approach            | ETL Use Case                                      |
| ------------------- | ------------------------------------------------- |
| ThreadPoolExecutor  | Parallel API calls, file downloads, DB queries    |
| ProcessPoolExecutor | CPU-heavy transforms, ML scoring                  |
| `Queue`             | Thread-safe producer-consumer pipeline            |
| `Lock`              | Thread-safe counters and shared state             |
| `as_completed()`    | Non-blocking parallel extraction + error handling |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Using multiprocessing for I/O-bound tasks (extra overhead)
with ProcessPoolExecutor() as pool:
    results = list(pool.map(fetch_from_api, urls))  # Overkill!

# RIGHT: Use threads for I/O
with ThreadPoolExecutor() as pool:
    results = list(pool.map(fetch_from_api, urls))

# WRONG: Forgetting 'if __name__ == "__main__"' with multiprocessing
with ProcessPoolExecutor() as pool:  # On macOS/Windows, this spawns subprocesses
    results = pool.map(heavy_transform, chunks)  # CRASH without the guard!

# RIGHT:
if __name__ == "__main__":
    with ProcessPoolExecutor() as pool:
        results = pool.map(heavy_transform, chunks)

# WRONG: Accessing shared mutable state in threads without a lock
total = 0
def increment():
    global total
    total += 1  # Race condition! Not thread-safe
```
