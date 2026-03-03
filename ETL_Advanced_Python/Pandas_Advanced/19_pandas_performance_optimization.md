# ⚡ Pandas Performance Optimization (ETL Context)

---

## 🤔 Why Does Performance Matter?

A poorly optimized Pandas script that takes 10 minutes on 100K rows will take **100+ minutes on 10M rows**. Understanding performance patterns is essential for production ETL.

> 💡 **Golden Rule**: The fastest Python code is code that runs as few Python instructions as possible. Vectorized Pandas/NumPy operations run in compiled C — orders of magnitude faster than Python loops.

---

## 🧱 Performance Hierarchy (Fastest → Slowest)

```
1. Vectorized Pandas ops:  df["a"] + df["b"]           → NumPy/C speed
2. df.apply(np.func)                                   → nearly vectorized
3. df.itertuples()                                     → 10x slower than vectorized
4. df.iterrows()                                       → 100x slower — AVOID!
5. list comprehension over df.to_dict("records")       → acceptable for simple tasks
```

---

## 💻 Example 1: Memory Optimization with Dtypes

```python
import pandas as pd, numpy as np

df = pd.DataFrame({
    "order_id": range(1_000_000),
    "amount":   [99.99] * 1_000_000,
    "region":   ["NORTH"] * 500_000 + ["SOUTH"] * 500_000,
    "status":   ["ACTIVE"] * 700_000 + ["INACTIVE"] * 300_000,
    "qty":      [5] * 1_000_000,
})

print(f"Before: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

df["order_id"] = df["order_id"].astype(np.int32)   # int64 → int32: saves 50%
df["amount"]   = df["amount"].astype(np.float32)   # float64 → float32: saves 50%
df["region"]   = df["region"].astype("category")   # object → category: huge savings
df["status"]   = df["status"].astype("category")   # 2 unique values → tiny!
df["qty"]      = df["qty"].astype(np.int8)         # Range -128 to 127 fits int8

print(f"After:  {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
# Typically 60-80% memory reduction!
```

---

## 💻 Example 2: Vectorized vs. Loop Patterns

```python
import pandas as pd, numpy as np, time

df = pd.DataFrame({
    "price":    np.random.uniform(10, 500, 1_000_000),
    "qty":      np.random.randint(1, 100, 1_000_000),
    "discount": np.random.uniform(0, 0.3, 1_000_000),
    "region":   np.random.choice(["NORTH","SOUTH","EAST","WEST"], 1_000_000),
})

# ❌ NEVER: iterrows (1M rows → ~60 seconds)
# for _, row in df.iterrows():
#     row["price"] * row["qty"]   # Python-level, extremely slow

# ✅ Vectorized arithmetic (1M rows → ~0.01s)
df["revenue"] = df["price"] * df["qty"] * (1 - df["discount"])

# ✅ np.where for one condition
df["premium"] = np.where(df["revenue"] > 5000, True, False)

# ✅ np.select for multiple conditions (SQL CASE WHEN equivalent)
conditions = [df["revenue"] > 10000, df["revenue"] > 5000, df["revenue"] > 1000]
choices    = ["Platinum", "Gold", "Silver"]
df["tier"] = np.select(conditions, choices, default="Bronze")
```

---

## 💻 Example 3: `read_csv` Best Practices

```python
import pandas as pd

# Optimized CSV reading
df = pd.read_csv(
    "large_file.csv",
    usecols       = ["id", "amount", "region", "date"],  # Only needed cols
    dtype         = {"id": "int32", "amount": "float32"},# Pre-specify dtypes
    parse_dates   = ["date"],                             # Parse dates on read
    on_bad_lines  = "skip",                               # Skip malformed rows
    low_memory    = False,                                # Avoids mixed-type warnings
)
```

---

## 💻 Example 4: Chunk-Based Processing for Huge Files

```python
import pandas as pd

def process_large_file(filepath: str, output_path: str, chunk_size: int = 50_000):
    """Process a file too large to fit in memory by chunking."""
    chunks = []
    for i, chunk in enumerate(pd.read_csv(filepath, chunksize=chunk_size)):
        # Transform
        chunk["revenue"] = chunk["price"] * chunk["qty"]
        chunk = chunk[chunk["revenue"] > 0]              # Filter
        chunks.append(chunk)
        if i % 10 == 0:
            print(f"Batch {i}: {len(chunk):,} rows")

    result = pd.concat(chunks, ignore_index=True)
    result.to_parquet(output_path, compression="snappy")
    print(f"✅ Written {len(result):,} rows to {output_path}")
```

---

## 💻 Example 5: `df.query()` — Readable Fast Filtering

```python
import pandas as pd

df = pd.DataFrame({
    "region": ["NORTH","SOUTH","EAST","NORTH","WEST"] * 200_000,
    "amount": [1500, 200, 800, 3000, 500] * 200_000,
    "status": ["ACTIVE","INACTIVE","ACTIVE","ACTIVE","PENDING"] * 200_000,
})

# Standard boolean indexing
result = df[
    df["region"].isin(["NORTH","EAST"]) &
    (df["amount"] > 1000) &
    (df["status"] == "ACTIVE")
]

# Equivalent with query() — cleaner and often faster
result = df.query("region in ['NORTH','EAST'] and amount > 1000 and status == 'ACTIVE'")

# Use @ prefix for local Python variables inside query string
min_amt = 1000
result  = df.query("amount > @min_amt")
```

---

## 🏭 ETL Use Cases Summary

| Technique                        | Memory Savings               | Speed Impact               |
| -------------------------------- | ---------------------------- | -------------------------- |
| `astype("category")` for strings | Up to 90%                    | Faster groupby/filter      |
| `float32` instead of `float64`   | 50%                          | Slight improvement         |
| `usecols=` in read_csv           | Proportional to skipped cols | Faster read                |
| Vectorized arithmetic            | —                            | 100x vs iterrows           |
| `np.select` vs apply             | —                            | 10-50x                     |
| Parquet output                   | 3-10x smaller                | 10x faster reads next time |

---

## ⚠️ Common Mistakes

```python
# ❌ Using iterrows for computation
for idx, row in df.iterrows():
    df.at[idx, "total"] = row["price"] * row["qty"]   # Very slow!
# ✅
df["total"] = df["price"] * df["qty"]

# ❌ Loading an entire huge table, then filtering
df = pd.read_sql("SELECT * FROM fact_sales", engine)   # OOM risk!
df = df[df["year"] == 2024]
# ✅ Push filter to the DB
df = pd.read_sql("SELECT * FROM fact_sales WHERE year = 2024", engine)
```
