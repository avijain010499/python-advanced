# ⚡ Pandas Performance Optimization (ETL Context)

---

## 📖 Explanation

Performance is critical in ETL — especially when processing millions of rows. Pandas has several strategies to dramatically speed up operations: choosing the right data types, leveraging vectorized operations, using chunked processing, and using libraries like `Dask` or `Polars` for scale.

---

## 🧠 Must-Remember Points

- **Rule #1**: Never iterate rows with `for row in df.iterrows()` — 100x slower than vectorized ops.
- Prefer NumPy/Pandas vectorized operations over `.apply(axis=1)`.
- Use the **smallest correct dtype** for each column (saves memory and speeds up ops).
- `pd.Categorical` for low-cardinality string columns (huge memory savings).
- `pd.to_numeric(errors='coerce')` for safe numeric conversion.
- `df.memory_usage(deep=True)` shows actual memory per column.
- `chunksize` in `read_csv()` / `read_sql()` for huge files.
- `df.query()` can be faster than boolean indexing for complex conditions.
- `pd.eval()` evaluates string expressions using numexpr (faster for arithmetic).
- Prefer `inplace=False` for method chaining; `inplace=True` can prevent copy optimizations.
- **Parquet > CSV** for storage and read performance.

---

## 💻 Code Examples

### 1️⃣ Memory Optimization with Proper Data Types

```python
import pandas as pd
import numpy as np

# Simulate a DataFrame with poor dtypes
df = pd.DataFrame({
    "order_id": range(1_000_000),              # int64
    "amount": [99.99] * 1_000_000,            # float64
    "region": ["NORTH"] * 500_000 + ["SOUTH"] * 500_000,  # object
    "status": ["ACTIVE"] * 700_000 + ["INACTIVE"] * 300_000,
    "qty": [5] * 1_000_000,                   # int64
})

print("Before optimization:")
print(df.memory_usage(deep=True).sum() / 1024**2, "MB")

# Optimize dtypes
df["order_id"] = df["order_id"].astype(np.int32)    # int64→int32 saves 50%
df["amount"] = df["amount"].astype(np.float32)       # float64→float32 saves 50%
df["region"] = df["region"].astype("category")       # object→category: huge savings
df["status"] = df["status"].astype("category")
df["qty"] = df["qty"].astype(np.int8)               # Range -128 to 127

print("\nAfter optimization:")
print(df.memory_usage(deep=True).sum() / 1024**2, "MB")
print(df.dtypes)
```

---

### 2️⃣ Vectorized Operations vs Slow Patterns

```python
import time
import pandas as pd
import numpy as np

df = pd.DataFrame({
    "price": np.random.uniform(10, 500, 1_000_000),
    "qty": np.random.randint(1, 100, 1_000_000),
    "discount": np.random.uniform(0, 0.3, 1_000_000),
    "region": np.random.choice(["NORTH", "SOUTH", "EAST", "WEST"], 1_000_000),
})

# SLOW: iterrows (never use!)
start = time.time()
revenues_slow = []
for _, row in df.head(10_000).iterrows():
    revenues_slow.append(row["price"] * row["qty"] * (1 - row["discount"]))
print(f"iterrows (10K rows): {time.time()-start:.3f}s")

# FAST: vectorized
start = time.time()
df["revenue"] = df["price"] * df["qty"] * (1 - df["discount"])
print(f"Vectorized (1M rows): {time.time()-start:.3f}s")  # Multiple times faster

# FAST: np.where vs apply for conditionals
df["tier"] = np.where(df["revenue"] > 10000, "Premium",
             np.where(df["revenue"] > 5000, "Standard", "Basic"))

# FAST: np.select for multiple conditions
conditions = [df["revenue"] > 10000, df["revenue"] > 5000, df["revenue"] > 1000]
choices = ["Platinum", "Gold", "Silver"]
df["tier"] = np.select(conditions, choices, default="Bronze")
```

---

### 3️⃣ Chunk-Based Processing for Large Files

```python
import pandas as pd

def process_large_csv(filepath: str, output_path: str, chunk_size: int = 50_000):
    """Process a large CSV file in memory-efficient chunks."""
    processed_chunks = []
    total_rows = 0

    for i, chunk in enumerate(pd.read_csv(filepath, chunksize=chunk_size)):
        # Transform
        chunk["revenue"] = chunk["price"] * chunk["qty"]
        chunk = chunk[chunk["revenue"] > 0]
        chunk["region"] = chunk["region"].str.upper().str.strip()
        processed_chunks.append(chunk)
        total_rows += len(chunk)

        if i % 10 == 0:
            print(f"Processed chunk {i}, rows so far: {total_rows}")

    # Combine and write
    result = pd.concat(processed_chunks, ignore_index=True)
    result.to_parquet(output_path, index=False, compression="snappy")
    print(f"Done: {total_rows} rows written to {output_path}")
```

---

### 4️⃣ `df.query()` — Faster Complex Filtering

```python
df = pd.DataFrame({
    "region": ["NORTH", "SOUTH", "EAST", "NORTH", "WEST"] * 200_000,
    "amount": [1500, 200, 800, 3000, 500] * 200_000,
    "status": ["ACTIVE", "INACTIVE", "ACTIVE", "ACTIVE", "PENDING"] * 200_000,
})

# Standard boolean indexing
result = df[(df["region"].isin(["NORTH", "EAST"])) & (df["amount"] > 1000) & (df["status"] == "ACTIVE")]

# Equivalent with query() — often faster and more readable
result = df.query("region in ['NORTH', 'EAST'] and amount > 1000 and status == 'ACTIVE'")

# Using local variables in query with @
min_amount = 1000
regions = ["NORTH", "EAST"]
result = df.query("region in @regions and amount > @min_amount and status == 'ACTIVE'")
```

---

### 5️⃣ Efficient Column Selection and Copy Avoidance

```python
# Avoid loading unnecessary columns during read
df = pd.read_csv(
    "large_file.csv",
    usecols=["id", "amount", "region", "date"],  # Only needed columns
    dtype={"id": "int32", "amount": "float32"},   # Pre-specify dtypes
    parse_dates=["date"],                          # Parse dates on read
)

# Avoid creating copies — chain operations
df = (
    pd.read_csv("data.csv", usecols=["id", "name", "amount"])
    .rename(columns={"id": "order_id"})
    .assign(
        name=lambda df: df["name"].str.strip().str.title(),
        amount=lambda df: pd.to_numeric(df["amount"], errors="coerce").fillna(0),
        revenue_tier=lambda df: pd.cut(
            df["amount"], bins=[0, 500, 2000, float("inf")],
            labels=["Low", "Mid", "High"]
        )
    )
    .dropna()
    .reset_index(drop=True)
)
```

---

### 6️⃣ Profiling and Benchmarking

```python
import time
import pandas as pd

# Time a block
def time_operation(label, func, *args, **kwargs):
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    print(f"{label}: {elapsed:.4f}s")
    return result

# Memory profiling
def memory_report(df: pd.DataFrame) -> None:
    usage = df.memory_usage(deep=True)
    total = usage.sum()
    print(f"\nMemory Usage Report:")
    print(f"  Total: {total / 1024**2:.2f} MB ({len(df):,} rows)")
    for col, mem in usage.items():
        if col != "Index":
            dtype = df[col].dtype if col in df.columns else "index"
            print(f"  {col:30s}: {mem/1024**2:.3f} MB ({dtype})")
```

---

## 🏭 ETL Use Cases

| Technique               | ETL Use Case                                          |
| ----------------------- | ----------------------------------------------------- |
| `astype("category")`    | Low-cardinality string columns (status, region, dept) |
| `np.int32` / `float32`  | Reduce memory of numeric columns by 50%               |
| `read_csv(usecols=...)` | Skip loading unused columns                           |
| Chunk processing        | Process files larger than RAM                         |
| `df.query()`            | Fast, readable multi-condition filtering              |
| Vectorized ops          | Replace row-wise `apply(axis=1)`                      |
| Parquet output          | 10x faster reads, 3x smaller files than CSV           |

---

## ⚠️ Common Pitfalls

```python
# WRONG: iterrows in a loop (extraordinarily slow)
for idx, row in df.iterrows():
    df.at[idx, "total"] = row["price"] * row["qty"]  # O(N) per row!

# RIGHT: Vectorized
df["total"] = df["price"] * df["qty"]

# WRONG: df[col] = df[col].apply(str.upper) on 1M rows
df["name"] = df["name"].apply(str.upper)  # Slow — Python-level loop

# RIGHT: .str.upper() — vectorized
df["name"] = df["name"].str.upper()

# WRONG: Loading entire large table then filtering
df = pd.read_sql("SELECT * FROM billion_row_table", engine)  # OOM!
df = df[df["status"] == "ACTIVE"]

# RIGHT: Push filter to database
df = pd.read_sql("SELECT * FROM billion_row_table WHERE status = 'ACTIVE'", engine)
```
