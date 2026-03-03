# ⚡ Multithreading & Multiprocessing in Python (ETL Context)

---

## 🤔 What Is Concurrency and Why Does It Matter?

Imagine your ETL pipeline needs to fetch data from 10 different APIs. Sequentially, it takes 10 seconds (1 sec each). With concurrency, all 10 run **at the same time** — total time ≈ 1 second.

Python offers two concurrency models:

| Model               | Best For                                              | Key Point                                |
| ------------------- | ----------------------------------------------------- | ---------------------------------------- |
| **Multithreading**  | I/O-bound tasks (file reads, API calls, DB queries)   | Threads share memory; limited by the GIL |
| **Multiprocessing** | CPU-bound tasks (heavy computation, large transforms) | Separate processes; bypasses the GIL     |

> 💡 **The GIL (Global Interpreter Lock)**: Python only lets ONE thread run Python code at a time. This means threading doesn't speed up CPU-heavy work. But for I/O (waiting for network/disk), threads ARE effective because they wait efficiently.

---

## 🧱 `concurrent.futures` — The High-Level API

The easiest way to use both threading and multiprocessing. Use `ThreadPoolExecutor` for I/O, `ProcessPoolExecutor` for CPU.

```python
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

# ThreadPoolExecutor — I/O-bound (API calls, DB queries)
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(my_function, arg): arg for arg in my_args}
    for future in as_completed(futures):
        result = future.result()   # Get the return value

# ProcessPoolExecutor — CPU-bound (heavy computation)
with ProcessPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(heavy_function, chunks))  # Returns results in order
```

---

## 💻 Example 1: Sequential vs Parallel API Calls

```python
import time

def fetch_api(endpoint):
    """Simulate fetching data from an API — takes 1 second (I/O wait)."""
    time.sleep(1)
    return {"endpoint": endpoint, "records": 100}

endpoints = [f"https://api.example.com/{name}"
             for name in ["sales", "orders", "customers", "products", "inventory"]]

# ---- SEQUENTIAL: ~5 seconds ----
start = time.time()
results_seq = [fetch_api(ep) for ep in endpoints]
print(f"Sequential: {time.time()-start:.1f}s")   # ~5.0s

# ---- PARALLEL with ThreadPoolExecutor: ~1 second ----
from concurrent.futures import ThreadPoolExecutor, as_completed

start = time.time()
results_par = []
with ThreadPoolExecutor(max_workers=5) as executor:
    future_to_ep = {executor.submit(fetch_api, ep): ep for ep in endpoints}
    for future in as_completed(future_to_ep):
        ep = future_to_ep[future]
        try:
            result = future.result()
            results_par.append(result)
            print(f"  ✅ {ep}: {result['records']} records")
        except Exception as e:
            print(f"  ❌ {ep} failed: {e}")

print(f"Parallel: {time.time()-start:.1f}s")   # ~1.0s
```

---

## 💻 Example 2: Parallel CPU-Bound Transforms

```python
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

def transform_chunk(records):
    """Heavy transformation — runs in a separate process."""
    df = pd.DataFrame(records)
    df["revenue"]  = df["price"] * df["qty"]
    df["margin"]   = (df["revenue"] - df["cost"]) / df["revenue"]
    return df.to_dict("records")

# Split large dataset into chunks for parallel processing
all_records = [{"price": i, "qty": 5, "cost": i*0.6} for i in range(1, 100001)]
chunk_size  = 25000
chunks = [all_records[i:i+chunk_size] for i in range(0, len(all_records), chunk_size)]

if __name__ == "__main__":   # REQUIRED on macOS/Windows for multiprocessing!
    with ProcessPoolExecutor(max_workers=4) as executor:
        processed_chunks = list(executor.map(transform_chunk, chunks))

    # Flatten results
    final = [row for chunk in processed_chunks for row in chunk]
    print(f"✅ Transformed {len(final):,} records using 4 processes")
```

---

## 💻 Example 3: Thread-Safe Queue — Producer/Consumer Pattern

```python
import threading
from queue import Queue
import time

# Shared queues between threads
extract_queue = Queue(maxsize=50)   # Limits memory — pauses producer if full
load_queue    = Queue(maxsize=50)

def extractor():
    """Producer: Reads data and puts it into the extract queue."""
    for i in range(20):
        record = {"id": i, "value": i * 10}
        extract_queue.put(record)    # Blocks if queue is full
        time.sleep(0.05)
    extract_queue.put(None)          # Sentinel: signals "I'm done"

def transformer():
    """Consumer + Producer: Transforms and passes to load queue."""
    while True:
        record = extract_queue.get()
        if record is None:
            load_queue.put(None)     # Pass sentinel downstream
            break
        transformed = {**record, "value": record["value"] * 2}
        load_queue.put(transformed)

def loader():
    """Consumer: Loads transformed records."""
    loaded = []
    while True:
        record = load_queue.get()
        if record is None:
            break
        loaded.append(record)
    print(f"✅ Loaded {len(loaded)} records")

# Start all three stages concurrently
threads = [
    threading.Thread(target=extractor),
    threading.Thread(target=transformer),
    threading.Thread(target=loader),
]
for t in threads:
    t.start()
for t in threads:
    t.join()   # Wait for all to finish
```

---

## 🏭 ETL Use Cases Summary

| Approach              | ETL Use Case                                         |
| --------------------- | ---------------------------------------------------- |
| `ThreadPoolExecutor`  | Parallel API calls, file downloads, DB reads         |
| `ProcessPoolExecutor` | Heavy pandas transforms, ML scoring                  |
| `Queue`               | Thread-safe producer → transformer → loader pipeline |

---

## 🧠 Must-Remember Points

| Rule                         | Remember                                                 |
| ---------------------------- | -------------------------------------------------------- |
| GIL limits CPU threads       | Use processes for CPU-bound; threads for I/O-bound       |
| `if __name__ == "__main__":` | **Required** for `ProcessPoolExecutor` on Windows/macOS  |
| `as_completed()`             | Process results as they finish (not in submission order) |
| `executor.map()`             | Preserves order; blocks until all done                   |
| `Queue`                      | Thread-safe — use instead of plain lists across threads  |

---

## ⚠️ Common Mistakes

```python
# ❌ Using ProcessPoolExecutor for I/O (wasteful overhead)
with ProcessPoolExecutor() as pool:
    results = list(pool.map(fetch_api, urls))   # Overkill for I/O!
# ✅ Use ThreadPoolExecutor for I/O
with ThreadPoolExecutor() as pool:
    results = list(pool.map(fetch_api, urls))

# ❌ Forgetting if __name__ == "__main__" with multiprocessing
with ProcessPoolExecutor() as pool:   # Crashes on Windows/macOS without the guard!
    results = pool.map(transform, chunks)
# ✅ Always guard multiprocessing code
if __name__ == "__main__":
    with ProcessPoolExecutor() as pool:
        results = pool.map(transform, chunks)
```
