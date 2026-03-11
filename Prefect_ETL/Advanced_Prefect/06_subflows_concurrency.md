# 🔀 Subflows & Concurrent Task Mapping in Prefect

---

## 💻 Example 1: Subflows — Nesting Flows Within Flows

```python
from prefect import flow, task

@task
def extract(region: str) -> list:
    print(f"Extracting {region} data")
    return [{"region": region, "amount": 100 * (ord(region[0]) % 10)}]

@task
def transform(records: list) -> list:
    return [r for r in records if r["amount"] > 0]

@task
def load(records: list, destination: str) -> int:
    print(f"Loading {len(records)} records to {destination}")
    return len(records)

# Subflow: a complete pipeline for ONE region
@flow(name="Region ETL")
def region_etl(region: str) -> int:
    raw   = extract(region)
    clean = transform(raw)
    count = load(clean, f"warehouse/{region}")
    return count

# Parent flow: orchestrates all region subflows
@flow(name="Global ETL Pipeline", log_prints=True)
def global_etl():
    regions = ["NORTH", "SOUTH", "EAST", "WEST"]

    # Run each region subflow sequentially
    total = 0
    for region in regions:
        count = region_etl(region)   # Subflow call
        total += count

    print(f"Total records loaded: {total}")
    return total

# global_etl()
```

---

## 💻 Example 2: `.map()` — Run Task for Multiple Inputs in Parallel

```python
from prefect import flow, task

@task(name="Process Region", retries=2)
def process_region(region: str) -> dict:
    import time
    time.sleep(0.5)  # Simulate work
    return {"region": region, "records": 1000, "status": "success"}

@flow(name="Parallel Region Processing", log_prints=True)
def parallel_etl():
    regions = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]

    # .map() submits one task per region — all run in PARALLEL
    futures = process_region.map(regions)

    # .result() on a mapped result returns a list
    results = [f.result() for f in futures]

    total = sum(r["records"] for r in results)
    print(f"Processed {len(results)} regions, {total:,} total records")
    return results

# parallel_etl()
print("map() = submit one task per item in the list, all in parallel")
```

---

## 💻 Example 3: Mixed Parallel and Sequential Tasks

```python
from prefect import flow, task

@task
def extract_sales()     -> list: return [1, 2, 3]
@task
def extract_customers() -> list: return [4, 5, 6]
@task
def extract_products()  -> list: return [7, 8, 9]
@task
def join_data(sales, customers, products) -> list:
    return sales + customers + products
@task
def run_quality_checks(data: list) -> bool:
    return len(data) > 0
@task
def load_to_warehouse(data: list) -> None:
    print(f"Loading {len(data)} records")
@task
def send_report(count: int) -> None:
    print(f"Report: {count} records loaded")

@flow(name="Mixed Parallel and Sequential ETL")
def complex_etl():
    # Phase 1: Extract in PARALLEL (all 3 start at the same time)
    s = extract_sales.submit()
    c = extract_customers.submit()
    p = extract_products.submit()

    # Phase 2: Join AFTER all 3 complete (sequential dependency)
    joined = join_data(s.result(), c.result(), p.result())

    # Phase 3: Quality check and load in PARALLEL
    qc_future   = run_quality_checks.submit(joined)
    load_future = load_to_warehouse.submit(joined)

    # Phase 4: Report AFTER both complete
    if qc_future.result():
        send_report(len(joined))
    else:
        print("❌ Quality checks failed!")

# complex_etl()
```

---

## 💻 Example 4: Dynamic Task Mapping with Multiple Arguments

```python
from prefect import flow, task

@task(name="Transform Batch")
def transform_batch(records: list, batch_size: int, threshold: float) -> list:
    return [r for r in records[:batch_size] if r > threshold]

@flow(name="Dynamic Map ETL")
def dynamic_map_etl():
    # Different data chunks
    data_chunks = [[1,2,3,4,5], [10,20,30], [100,200,300,400]]

    # Same size and threshold for all
    results = transform_batch.map(
        data_chunks,
        batch_size = 3,     # Unmapped = same value for all mapped calls
        threshold  = 5.0,
    )

    all_results = [r for future in results for r in future.result()]
    print(f"Total: {len(all_results)} records")

# dynamic_map_etl()
print("Unmapped args = same value broadcast to all parallel tasks")
```

---

## 🏭 Summary

| Pattern                   | When to Use                                              |
| ------------------------- | -------------------------------------------------------- |
| Subflows                  | Modular, reusable pipelines — one per region/source      |
| `.submit()`               | Parallel tasks — submit all, wait later with `.result()` |
| `.map(list)`              | Same task × multiple inputs — parallel fan-out           |
| Sequential call           | When step B needs step A's output                        |
| Mixed parallel/sequential | Most real ETL pipelines — extract parallel → join → load |
