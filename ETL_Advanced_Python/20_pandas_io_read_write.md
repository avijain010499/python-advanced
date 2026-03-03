# 💾 Pandas I/O — Read & Write (ETL Context)

---

## 📖 Explanation

Pandas has rich support for reading and writing multiple file formats — CSV, JSON, Excel, Parquet, Feather, SQL, and more. Choosing the right format and options dramatically impacts ETL performance and reliability.

---

## 🧠 Must-Remember Points

- **Parquet** is the standard format for ETL/data lake pipelines — columnar, compressed, fast.
- Always specify `encoding="utf-8"` and `dtype=` when reading CSVs for correctness and speed.
- `error_bad_lines=False` is deprecated — use `on_bad_lines="skip"` (Pandas 1.3+).
- `chunksize` in `read_csv()` and `read_sql()` enables memory-efficient processing.
- `df.to_sql(if_exists="append")` for incremental loads; `"replace"` for full loads.
- `df.to_sql(method="multi")` or `method=psycopg2.extras.execute_values` for fast DB inserts.
- `pd.read_excel(sheet_name=None)` reads all sheets as a dict of DataFrames.
- `pd.ExcelWriter` with `openpyxl` engine allows writing multiple sheets.
- `nrows=` in `read_csv()` for sampling/previewing large files.
- For JSON, `orient="records"` is the most common ETL-friendly orientation.
- Parquet supports column pruning and predicate pushdown — huge performance gains.

---

## 💻 Code Examples

### 1️⃣ Reading CSV Files (Best Practices)

```python
import pandas as pd

# Full options for production-grade CSV reading
df = pd.read_csv(
    "sales_data.csv",
    sep=",",                          # or "\t" for TSV, "|" for pipe
    encoding="utf-8",                  # Always explicit
    dtype={                            # Pre-specify dtypes (faster + safer)
        "order_id": "string",
        "customer_id": "int32",
        "amount": "float32",
    },
    parse_dates=["order_date"],        # Parse date columns
    date_format="%Y-%m-%d",           # Explicit format (faster)
    usecols=["order_id", "customer_id", "amount", "order_date"],  # Only needed cols
    skiprows=1,                        # Skip header rows if needed
    na_values=["N/A", "NULL", "", "none", "None", "-"],  # Treat as NaN
    on_bad_lines="skip",               # Skip malformed rows (Pandas 1.3+)
    low_memory=False,                  # Prevents mixed-type inference warnings
    thousands=",",                     # Handle "1,500,000" format
    thousands_sep=",",
)

print(f"Loaded {len(df):,} rows | Memory: {df.memory_usage(deep=True).sum()/1024**2:.2f} MB")
print(df.dtypes)
```

---

### 2️⃣ Writing CSV Files

```python
# Write clean CSV
df.to_csv(
    "output/cleaned_sales.csv",
    index=False,          # Don't write row numbers
    encoding="utf-8",
    date_format="%Y-%m-%d",
    float_format="%.2f",  # 2 decimal places for floats
    na_rep="",            # Write empty string for NaN
)

# Write gzip-compressed CSV (save ~70% space)
df.to_csv("output/sales.csv.gz", index=False, compression="gzip")

# Write pipe-delimited (for legacy systems)
df.to_csv("output/sales_pipe.txt", sep="|", index=False)
```

---

### 3️⃣ Parquet — The ETL Standard Format

```python
import pandas as pd

# Write Parquet
df.to_parquet(
    "output/sales.parquet",
    engine="pyarrow",      # or "fastparquet"
    index=False,
    compression="snappy",  # Good balance of speed/size; "gzip" for max compression
)

# Read Parquet — much faster than CSV
df = pd.read_parquet("output/sales.parquet", engine="pyarrow")

# Column pruning — only read needed columns (Parquet native feature)
df_subset = pd.read_parquet("output/sales.parquet", columns=["order_id", "amount"])

# Partition-based Parquet (common in data lakes: Hive-style partitioning)
# Write partitioned Parquet dataset
import pyarrow as pa
import pyarrow.parquet as pq

table = pa.Table.from_pandas(df)
pq.write_to_dataset(
    table,
    root_path="output/sales_partitioned/",
    partition_cols=["region", "year"],  # Partition by region and year
)

# Read specific partition
df_north = pd.read_parquet("output/sales_partitioned/region=NORTH/")
```

---

### 4️⃣ JSON — Reading and Writing

```python
import pandas as pd

# Read JSON (various orientations)
# orient="records": [{"col1": val1, "col2": val2}, ...]  ← ETL standard
df = pd.read_json("data.json", orient="records")

# Read JSON Lines (ndjson) — one record per line (streaming-friendly)
df = pd.read_json("data.jsonl", lines=True)

# Write JSON
df.to_json("output.json", orient="records", indent=2, date_format="iso")

# Write JSON Lines
df.to_json("output.jsonl", orient="records", lines=True)

# Handle nested JSON (from APIs)
import json
with open("nested_data.json") as f:
    raw = json.load(f)

# Normalize nested JSON to flat DataFrame
df_flat = pd.json_normalize(
    raw["data"],
    record_path=["orders"],       # Path to list of records
    meta=["customer_id", "region"],  # Parent fields to include
    sep="_",                      # Nested field separator: order.amount → order_amount
)
```

---

### 5️⃣ Excel — Reading and Writing

```python
import pandas as pd

# Read one sheet
df = pd.read_excel("report.xlsx", sheet_name="Sales", skiprows=2)

# Read all sheets (returns dict)
all_sheets = pd.read_excel("report.xlsx", sheet_name=None)
for sheet_name, sheet_df in all_sheets.items():
    print(f"Sheet: {sheet_name}, Rows: {len(sheet_df)}")

# Combine all sheets
df_all = pd.concat(all_sheets.values(), ignore_index=True)

# Write multiple DataFrames to multiple sheets
with pd.ExcelWriter("etl_output.xlsx", engine="openpyxl") as writer:
    df_sales.to_excel(writer, sheet_name="Sales", index=False)
    df_customers.to_excel(writer, sheet_name="Customers", index=False)
    summary_df.to_excel(writer, sheet_name="Summary", index=False)

print("Excel file written with 3 sheets.")
```

---

### 6️⃣ SQL — Reading and Writing with SQLAlchemy

```python
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("postgresql+psycopg2://user:pass@localhost/etl_db")

# Read — full table
df = pd.read_sql_table("sales", engine)

# Read — with query and chunking for large tables
query = """
    SELECT order_id, customer_id, amount, order_date, region
    FROM sales
    WHERE order_date >= '2024-01-01'
    AND status = 'ACTIVE'
"""
chunks = []
for chunk in pd.read_sql(query, engine, chunksize=50_000, parse_dates=["order_date"]):
    chunk_clean = chunk.dropna(subset=["order_id"])
    chunks.append(chunk_clean)
df = pd.concat(chunks, ignore_index=True)

# Write — full replace
df_transformed.to_sql(
    "sales_transformed",
    engine,
    if_exists="replace",   # 'replace' = drop + create + insert
    index=False,
    method="multi",         # Batch inserts
    chunksize=10_000,
)

# Write — append (incremental load)
df_new_records.to_sql(
    "sales",
    engine,
    if_exists="append",
    index=False,
    method="multi",
)
print(f"Appended {len(df_new_records)} records to sales table.")
```

---

### 7️⃣ Automatic Format Detection + ETL Loader Function

```python
from pathlib import Path
import pandas as pd

def read_etl_source(filepath: str, **kwargs) -> pd.DataFrame:
    """Auto-detect file format and read into DataFrame."""
    path = Path(filepath)
    ext = path.suffix.lower()

    readers = {
        ".csv": pd.read_csv,
        ".tsv": lambda f, **kw: pd.read_csv(f, sep="\t", **kw),
        ".txt": lambda f, **kw: pd.read_csv(f, sep="|", **kw),
        ".json": lambda f, **kw: pd.read_json(f, orient="records", **kw),
        ".jsonl": lambda f, **kw: pd.read_json(f, lines=True, **kw),
        ".parquet": pd.read_parquet,
        ".xlsx": pd.read_excel,
        ".xls": pd.read_excel,
    }

    reader = readers.get(ext)
    if not reader:
        raise ValueError(f"Unsupported file format: {ext}")

    df = reader(filepath, **kwargs)
    print(f"Read {len(df):,} rows from {path.name} ({ext})")
    return df

# Usage
df = read_etl_source("sales.parquet")
df = read_etl_source("customers.csv", encoding="utf-8")
df = read_etl_source("orders.xlsx", sheet_name="Sheet1")
```

---

## 🏭 ETL Use Cases

| Format / Method                    | ETL Use Case                                 |
| ---------------------------------- | -------------------------------------------- |
| `read_csv(dtype=..., usecols=...)` | Fast, memory-safe CSV ingestion              |
| `read_csv(chunksize=N)`            | Stream huge CSV files                        |
| Parquet + columnar read            | Data lake ingestion, fast analytical queries |
| JSON Lines                         | API response streaming, event data           |
| `read_sql(chunksize=N)`            | Extract large database tables                |
| `to_sql(if_exists="append")`       | Incremental ETL loads                        |
| `ExcelWriter`                      | Multi-sheet reporting outputs                |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Not specifying dtype — Pandas guesses (slow + may be wrong)
df = pd.read_csv("data.csv")

# RIGHT: Pre-specify dtypes for performance and correctness
df = pd.read_csv("data.csv", dtype={"id": "int32", "status": "string"})

# WRONG: Reading CSV with Excel (or vice versa) without checking format
df = pd.read_csv("report.xlsx")  # ValueError or garbled data!

# RIGHT: Check the extension
df = pd.read_excel("report.xlsx")  # Correct reader

# WRONG: to_sql with default method (individual INSERTs — very slow)
df.to_sql("target", engine, if_exists="append")  # N INSERT statements!

# RIGHT: Batch inserts
df.to_sql("target", engine, if_exists="append", method="multi", chunksize=5000)
```
